import sys
import os

print("Starting the script...")

# Add the path to smartapi package manually
sys.path.append('C:\\Users\\bhave\\AppData\\Roaming\\Python\\Python313\\site-packages')

print("Updated sys.path:", sys.path)

try:
    from smartapi import SmartConnect
    print("smartapi imported successfully!")
except ImportError as e:
    print(f"ImportError: {e}")
    print("Module not found in", sys.path)

print("Script finished executing")

