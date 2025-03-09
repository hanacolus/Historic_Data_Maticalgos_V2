import logging
from maticalgos.historical import historical

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ma = historical('jtutanota@gmail.com')

# ma.reset_password() 

# Attempt to login
try:
    ma.login("965124")
    logging.info("Login successful.")
except Exception as e:
    logging.error(f"Login failed: {e}") 
