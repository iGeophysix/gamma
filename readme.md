1. Setup docker and docker-compose
2. Build and run auxiliary services
   ```bash
   docker-compose up --build```
3. Credentials to be configured via env vars (see settings.py)
4. create venv
    ```bash
    python3 -m virtualenv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
5. run tests in PyCharm :-)