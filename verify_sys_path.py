import sys

print("Starting the script...")

# Manually add the correct path to SmartApi
smartapi_path = 'C:\Users\bhave\Documents\Astrotrade108\venv\Lib\site-packages\smartapi'
if smartapi_path not in sys.path:
    sys.path.append(smartapi_path)

print("Updated sys.path:", sys.path)

try:
    from smartapi.smartConnect import SmartConnect  # Corrected import path
    print("smartapi imported successfully!")
except ImportError as e:
    print(f"ImportError: {e}")
    print("Module not found in", sys.path)

print("Script finished executing")




