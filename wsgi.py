"""
WSGI entry point — used by gunicorn in Procfile.
"""
from app import create_app

app = create_app()

if __name__ == '__main__':
    import os
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
