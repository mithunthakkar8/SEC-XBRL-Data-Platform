from arelle import Cntlr
import psycopg2
import logging
from logging.handlers import RotatingFileHandler

# Configure logging
def setup_logging():
    logger = logging.getLogger('xbrl_processor')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    fh = RotatingFileHandler('xbrl_processor.log', maxBytes=10485760, backupCount=5)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger

logger = setup_logging()

db_config = {
    'dbname': 'finhub',
    'user': 'finhub_admin',
    'password': 'pass@123',
    'host': 'localhost',
    'port': '5432'
}


def process_xbrl_relationships(xsd_file_path, accession_number):
    logger.info(f"Starting XBRL relationship processing for {xsd_file_path}")
    
    cntlr = Cntlr.Cntlr()
    try:
        model_xbrl = cntlr.modelManager.load(xsd_file_path)
    except Exception as e:
        logger.error(f"Failed to load XBRL model: {str(e)}")
        raise

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        model_xbrl.close()
        raise

    try:
        # Get filing_id
        cursor.execute("SELECT filing_id FROM xbrl.filing WHERE accession_number = %s", (accession_number,))
        filing_id = cursor.fetchone()[0]
        logger.info(f"Processing filing ID: {filing_id}")

        # 1. Batch insert all concepts
        concept_data = set()
        for concept in model_xbrl.facts:
            qname = str(concept.qname)
            concept_name = qname.split(':')[-1]
            namespace = concept.qname.namespaceURI
            concept_data.add((concept_name, namespace))
        
        # Batch upsert concepts
        if concept_data:
            args_str = ','.join(cursor.mogrify("(%s,%s)", x).decode('utf-8') for x in concept_data)
            cursor.execute(f"""
                INSERT INTO xbrl.concept (concept_name, namespace)
                VALUES {args_str}
                ON CONFLICT (concept_name, namespace) 
                DO UPDATE SET concept_name = EXCLUDED.concept_name
            """)
            concept_count = len(concept_data)
            conn.commit()
            logger.info(f"Upserted {concept_count} concepts in batch")

        # 2. Process arcroles with batch upsert
        arcrole_data = [(uri,) for uri in model_xbrl.arcroleTypes]
        if arcrole_data:
            args_str = ','.join(cursor.mogrify("(%s)", x).decode('utf-8') for x in arcrole_data)
            cursor.execute(f"""
                INSERT INTO xbrl.arcrole (arcrole_uri)
                VALUES {args_str}
                ON CONFLICT (arcrole_uri) 
                DO UPDATE SET arcrole_uri = EXCLUDED.arcrole_uri
            """)
            conn.commit()
            logger.info(f"Upserted {len(arcrole_data)} arcroles in batch")

        # Preload all concepts into a dictionary for faster lookup
        cursor.execute("SELECT concept_name, namespace, concept_id FROM xbrl.concept")
        concept_map = {(name, ns): cid for name, ns, cid in cursor.fetchall()}

        # Preload all arcroles into a dictionary
        cursor.execute("SELECT arcrole_uri, arcrole_id FROM xbrl.arcrole")
        arcrole_map = {uri: aid for uri, aid in cursor.fetchall()}

        # Preload all roles into a dictionary and prepare for batch insert
        role_data = set()
        relationship_data = []
        dimension_declaration_data = set()
        dimension_member_data = set()
        hypercube_data = []
        dimension_relationship_data = []
        dimension_member_relationship_data = []

        # Collect all relationship data first
        for arcrole_uri in model_xbrl.arcroleTypes:
            rel_set = model_xbrl.relationshipSet(arcrole_uri)
            if not rel_set:
                logger.debug(f"No relationships for arcrole: {arcrole_uri}")
                continue

            arcrole_id = arcrole_map.get(arcrole_uri)
            if not arcrole_id:
                continue

            for rel in rel_set.modelRelationships:
                try:
                    # Collect role data
                    role_id = None
                    if hasattr(rel, 'linkrole') and rel.linkrole:
                        role_data.add((rel.linkrole,))
                    
                    # Get concept details
                    parent_qname = str(rel.fromModelObject.qname)
                    child_qname = str(rel.toModelObject.qname)
                    parent_name = parent_qname.split(':')[-1]
                    parent_ns = rel.fromModelObject.qname.namespaceURI
                    child_name = child_qname.split(':')[-1]
                    child_ns = rel.toModelObject.qname.namespaceURI

                    parent_key = (parent_name, parent_ns)
                    child_key = (child_name, child_ns)
                    
                    # Skip if concepts not found
                    if parent_key not in concept_map or child_key not in concept_map:
                        continue

                    # Prepare relationship data
                    relationship_data.append((
                        concept_map[parent_key], 
                        concept_map[child_key], 
                        arcrole_id,
                        rel.linkrole if hasattr(rel, 'linkrole') and rel.linkrole else None,
                        rel.order if hasattr(rel, 'order') else None,
                        rel.weight if hasattr(rel, 'weight') else None,
                        rel.preferredLabel if hasattr(rel, 'preferredLabel') else None,
                        filing_id
                    ))

                    # Prepare dimensional relationship data
                    if "dimension" in arcrole_uri:
                        if "hypercube-dimension" in arcrole_uri or "dimension-domain" in arcrole_uri:
                            dimension_declaration_data.add((parent_name,))
                        
                        if "dimension-domain" in arcrole_uri or "domain-member" in arcrole_uri:
                            dimension_member_data.add((parent_name, child_name))

                        if "hypercube-dimension" in arcrole_uri:
                            hypercube_data.append((
                                concept_map[parent_key],
                                rel.fromModelObject.isAbstract if hasattr(rel.fromModelObject, 'isAbstract') else None,
                                arcrole_id,
                                filing_id,
                                child_name,
                                child_ns
                            ))

                        elif "dimension-domain" in arcrole_uri or "domain-member" in arcrole_uri:
                            dimension_member_relationship_data.append((
                                parent_name,
                                parent_ns,
                                child_name,
                                arcrole_id,
                                filing_id
                            ))

                except Exception as e:
                    logger.warning(f"Skipping relationship: {str(e)}")
                    continue

        # Batch process roles
        if role_data:
            args_str = ','.join(cursor.mogrify("(%s)", x).decode('utf-8') for x in role_data)
            cursor.execute(f"""
                INSERT INTO xbrl.link_role (role_uri)
                VALUES {args_str}
                ON CONFLICT (role_uri) 
                DO UPDATE SET role_uri = EXCLUDED.role_uri
                RETURNING role_uri, role_id
            """)
            role_map = {uri: rid for uri, rid in cursor.fetchall()}
            conn.commit()
            logger.info(f"Upserted {len(role_data)} roles in batch")

        # Batch process relationships
        if relationship_data:
            # Update relationship data with role_ids
            relationship_data = [
                (p, c, a, role_map.get(r), o, w, pl, f) 
                for p, c, a, r, o, w, pl, f in relationship_data
            ]
            
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in relationship_data)
            cursor.execute(f"""
                INSERT INTO xbrl.concept_relationship (
                    parent_id, child_id, arcrole_id, role_id,
                    order_, weight, preferred_label, filing_id
                )
                VALUES {args_str}
                ON CONFLICT (parent_id, child_id, arcrole_id, role_id, filing_id) 
                DO UPDATE SET 
                    order_ = EXCLUDED.order_,
                    weight = EXCLUDED.weight,
                    preferred_label = EXCLUDED.preferred_label
            """)
            conn.commit()
            logger.info(f"Upserted {len(relationship_data)} relationships in batch")

        # Batch process dimensional data
        if dimension_declaration_data:
            args_str = ','.join(cursor.mogrify("(%s)", x).decode('utf-8') for x in dimension_declaration_data)
            cursor.execute(f"""
                INSERT INTO xbrl.dimension_declaration (dimension_name)
                VALUES {args_str}
                ON CONFLICT (dimension_name) 
                DO UPDATE SET dimension_name = EXCLUDED.dimension_name
                RETURNING dimension_name, dimension_id
            """)
            dimension_map = {name: did for name, did in cursor.fetchall()}
            conn.commit()
            logger.info(f"Upserted {len(dimension_declaration_data)} dimension declarations in batch")

        if dimension_member_data and dimension_map:
            # Filter to only include members for dimensions we know about
            dimension_member_data = [(d, m) for d, m in dimension_member_data if d in dimension_map]
            args_str = ','.join(cursor.mogrify("(%s,%s)", (dimension_map[d], m)).decode('utf-8') 
                          for d, m in dimension_member_data)
            cursor.execute(f"""
                INSERT INTO xbrl.dimension_member (dimension_id, member_name)
                VALUES {args_str}
                ON CONFLICT (dimension_id, member_name) 
                DO UPDATE SET member_name = EXCLUDED.member_name
                RETURNING dimension_id, member_name, member_id
            """)
            member_map = {(did, name): mid for did, name, mid in cursor.fetchall()}
            conn.commit()
            logger.info(f"Upserted {len(dimension_member_data)} dimension members in batch")

        if hypercube_data:
            # First insert hypercubes
            hypercube_insert_data = [(c, a) for c, a, *_ in hypercube_data]
            args_str = ','.join(cursor.mogrify("(%s,%s)", x).decode('utf-8') for x in hypercube_insert_data)
            cursor.execute(f"""
                INSERT INTO xbrl.hypercube (concept_id, is_abstract)
                VALUES {args_str}
                ON CONFLICT (concept_id) 
                DO UPDATE SET is_abstract = EXCLUDED.is_abstract
                RETURNING concept_id, hypercube_id
            """)
            hypercube_map = {cid: hid for cid, hid in cursor.fetchall()}
            
            # Then insert dimension relationships
            dim_rel_data = []
            for c, a, arcrole_id, filing_id, child_name, child_ns in hypercube_data:
                if c in hypercube_map:
                    dim_rel_data.append((
                        hypercube_map[c],
                        arcrole_id,
                        filing_id,
                        child_name,
                        child_ns
                    ))
            
            if dim_rel_data:
                args_str = ','.join(cursor.mogrify("""
                    (%s,%s,%s,(SELECT d.dimension_id FROM xbrl.dimension_declaration d
                               JOIN xbrl.concept c ON d.dimension_name = c.concept_name
                               WHERE c.concept_name = %s AND c.namespace = %s))
                """, x).decode('utf-8') for x in dim_rel_data)
                cursor.execute(f"""
                    INSERT INTO xbrl.dimension_relationship (
                        hypercube_id, arcrole_id, filing_id, dimension_id
                    )
                    VALUES {args_str}
                    ON CONFLICT (hypercube_id, dimension_id, arcrole_id, filing_id) 
                    DO UPDATE SET arcrole_id = EXCLUDED.arcrole_id
                """)
                conn.commit()
                logger.info(f"Upserted {len(dim_rel_data)} hypercube-dimension relationships in batch")

        if dimension_member_relationship_data and dimension_map and member_map:
            # Prepare the data for batch insert
            dim_mem_rel_insert_data = []
            for parent_name, parent_ns, child_name, arcrole_id, filing_id in dimension_member_relationship_data:
                if parent_name in dimension_map:
                    dim_id = dimension_map[parent_name]
                    mem_id = member_map.get((dim_id, child_name))
                    if mem_id:
                        dim_mem_rel_insert_data.append((
                            dim_id,
                            mem_id,
                            arcrole_id,
                            filing_id
                        ))
            
            if dim_mem_rel_insert_data:
                args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s)", x).decode('utf-8') for x in dim_mem_rel_insert_data)
                cursor.execute(f"""
                    INSERT INTO xbrl.dimension_member_relationship (
                        dimension_id, member_id, arcrole_id, filing_id
                    )
                    VALUES {args_str}
                    ON CONFLICT (dimension_id, member_id, arcrole_id, filing_id) 
                    DO UPDATE SET arcrole_id = EXCLUDED.arcrole_id
                """)
                conn.commit()
                logger.info(f"Upserted {len(dim_mem_rel_insert_data)} dimension-member relationships in batch")

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()
        model_xbrl.close()
        logger.info("Processing completed, resources released")

# Example usage
if __name__ == "__main__":
    process_xbrl_relationships(
        r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-K\2014-12-31\fcx-20141231_pre.xml",
        "0000831259-15-000016"
    )