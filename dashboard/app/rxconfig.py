import reflex as rx
import os
from dotenv import load_dotenv

# Read environment variables.
load_dotenv()

config = rx.Config(
    app_name="dfoh",
    frontend_port=3000,
    backend_port=8080,
    # api_url="https://bgproutes.io:8000",
    # api_url="http://localhost:8080",
    show_built_with_reflex=False
)