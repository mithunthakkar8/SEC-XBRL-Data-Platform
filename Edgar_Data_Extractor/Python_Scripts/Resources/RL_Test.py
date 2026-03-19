from arelle import Cntlr, ModelDocument
import psycopg2
import logging
from logging.handlers import RotatingFileHandler

xsd_file_path=r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-K\2014-12-31\fcx-20141231_pre.xml"
accession_number = "0000831259-15-000016"
db_config = {
    'dbname': 'finhub',
    'user': 'finhub_admin',
    'password': 'pass@123',
    'host': 'localhost',
    'port': '5432'
}

cntlr = Cntlr.Cntlr()
model_xbrl = cntlr.modelManager.load(xsd_file_path)

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

cursor.execute("SELECT filing_id FROM xbrl.filing WHERE accession_number = %s", (accession_number,))
filing_id = cursor.fetchone()[0]

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

# Preload all concepts into a dictionary for faster lookup
cursor.execute("SELECT concept_name, namespace, concept_id FROM xbrl.concept")
concept_map = {(name, ns): cid for name, ns, cid in cursor.fetchall()}

# Preload all arcroles into a dictionary
cursor.execute("SELECT arcrole_uri, arcrole_id FROM xbrl.arcrole")
arcrole_map = {uri: aid for uri, aid in cursor.fetchall()}

# Preload all roles into a dictionary and prepare for batch insert
link_role_data = set()
relationship_data = []
dimension_declaration_data = set()
dimension_member_data = set()
hypercube_data = []
dimension_relationship_data = []
dimension_member_relationship_data = []
all_data = []
notAll_data = []
dimension_default = []
fact_explanatoryFact = []

# Collect all relationship data first
for arcrole_uri in model_xbrl.arcroleTypes:
    print(f'Using arcrole: {arcrole_uri}')
    rel_set = model_xbrl.relationshipSet(arcrole_uri)

    arcrole_id = arcrole_map.get(arcrole_uri)
    if not arcrole_id:
        continue

    for rel in rel_set.modelRelationships:
        # Collect role data
        role_id = None
        if hasattr(rel, 'linkrole') and rel.linkrole:
            link_role_data.add((rel.linkrole,))
        
        # Get concept details
        parent_qname = str(rel.fromModelObject.qname)
        child_qname = str(rel.toModelObject.qname)
        parent_name = parent_qname.split(':')[-1]
        parent_ns = rel.fromModelObject.qname.namespaceURI
        child_name = child_qname.split(':')[-1]
        child_ns = rel.toModelObject.qname.namespaceURI
        
        if 'all' in arcrole_uri:
            print(f"Parent and childs for arcrole :{arcrole_uri} are parent: {parent_qname} and child: {child_qname}")

        parent_key = (parent_name, parent_ns)
        child_key = (child_name, child_ns)
                
        # Prepare relationship data
        relationship_data.append((
            parent_name,
            parent_ns,
            child_name,
            child_ns,
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
                dimension_member_relationship_data.append((
                    parent_name,
                    parent_ns,
                    child_name,
                    arcrole_id,
                    filing_id
                ))

            if "hypercube-dimension" in arcrole_uri:
                hypercube_data.append((
                    parent_name,
                    parent_ns,
                    rel.fromModelObject.isAbstract if hasattr(rel.fromModelObject, 'isAbstract') else None,
                    arcrole_id,
                    filing_id,
                    child_name,
                    child_ns
                ))
            
            if "all" in arcrole_uri:
                all_data.append((parent_name,
                    parent_ns,child_name,
                    child_ns))
                
            if "notAll" in arcrole_uri:
                notAll_data.append((parent_name,
                    parent_ns,child_name,
                    child_ns))
                
            if "default" in arcrole_uri:
                dimension_default.append((parent_name,
                    parent_ns,child_name,
                    child_ns))
            
            if "fact" in arcrole_uri:
                fact_explanatoryFact.append((parent_name,
                    parent_ns,child_name,
                    child_ns))


# Batch process roles
if link_role_data:
    args_str = ','.join(cursor.mogrify("(%s)", x).decode('utf-8') for x in link_role_data)
    cursor.execute(f"""
        INSERT INTO xbrl.link_role (role_uri)
        VALUES {args_str}
        ON CONFLICT (role_uri) 
        DO UPDATE SET role_uri = EXCLUDED.role_uri
        RETURNING role_uri, role_id
    """)
    role_map = {uri: rid for uri, rid in cursor.fetchall()}
    conn.commit()



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
        args_str_dim_rel = ','.join(cursor.mogrify("""
            (%s,%s,%s,(SELECT d.dimension_id FROM xbrl.dimension_declaration d
                        JOIN xbrl.concept c ON d.dimension_name = c.concept_name
                        WHERE c.concept_name = %s AND c.namespace = %s))
        """, x).decode('utf-8') for x in dim_rel_data)
        cursor.execute(f"""
            INSERT INTO xbrl.dimension_relationship (
                hypercube_id, arcrole_id, filing_id, dimension_id
            )
            VALUES {args_str_dim_rel}
            ON CONFLICT (hypercube_id, dimension_id, arcrole_id, filing_id) 
            DO UPDATE SET arcrole_id = EXCLUDED.arcrole_id
        """)
        conn.commit()

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
cursor.close()
conn.close()
model_xbrl.close()
