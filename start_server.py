import sys, os
sys.path.insert(0, "/home/columbo/ExtData/AIprojects/token-relay/src")
# Read API key from temp file
try:
    with open("/tmp/api_key.txt") as f:
        os.environ['HERMES_CUSTOM_LOCALHOST_20128_API_KEY'] = f.read().strip()
except:
    pass
from server import main
main()
