import sys
import os

# venv_path = os.path.abspath(os.path.join(os.getcwd(), "..", "Data_Extractor", "Lib", "site-packages"))
# if venv_path not in sys.path:
#     sys.path.insert(0, venv_path)

# check if the current environment is a virtual environment
print(sys.prefix)
print(sys.prefix != sys.base_prefix)
print(sys.executable)




