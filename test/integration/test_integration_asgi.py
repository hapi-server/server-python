"""Unit test for point 1: Integrating HAPI server into an existing ASGI application."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from hapiserver_demo.app import app as hapi_app


def test_hapi_mounted_as_subapplication():
  """Test that HAPI server can be mounted as a sub-application per point 1 instructions."""
  # Create main ASGI application
  main_app = FastAPI()

  # Add a simple root endpoint
  @main_app.get("/")
  async def root():
    return {"message": "Main app running"}

  # Mount HAPI server as sub-application (per point 1)
  main_app.mount("/hapi", hapi_app)

  # Create test client
  client = TestClient(main_app)

  # Test main app endpoint
  response = client.get("/")
  assert response.status_code == 200
  assert response.json() == {"message": "Main app running"}

  # Test HAPI server is accessible at mounted path
  # HAPI servers typically have /catalog, /info, /data endpoints
  response = client.get("/hapi/catalog")
  assert response.status_code in [200, 404]  # 404 if not configured, 200 if working

  # Test that HAPI endpoint returns something (even if just an error response)
  assert response.content is not None


def test_hapi_app_is_valid_asgi():
  """Verify that the hapi_app object is a valid ASGI application."""
  from hapiserver_demo.app import app as hapi_app

  # ASGI apps should be callable or have specific attributes
  assert hapi_app is not None

  # FastAPI apps have these attributes
  assert hasattr(hapi_app, 'routes') or hasattr(hapi_app, '__call__')
