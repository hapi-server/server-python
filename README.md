# Overview

`hapiserver` is Python HAPI server that uses FastAPI and a partial OpenAPI specification (see also the [`Java`](https://github.com/hapi-server/server-java) and [`NodeJS`](https://github.com/hapi-server/server-nodejs) generic HAPI servers). The last commit of the previous and deprecated version of this repository is given [commit 834790](https://github.com/hapi-server/server-python/commit/834790cbbf1d6b1e1016ff4488fa9fef28e7bd6b) and as [release 0.0.1](https://github.com/hapi-server/server-python/releases/tag/v0.0.1).

To create a HAPI server, a data provider must create four files

1. `config.json`
2. `catalog.py` with a function returns `/catalog` metadata
3. `info.py` with a function returns `/info` metadata
4. `data.py` with a function that returns the `/data` response

For a new project, we recommend cloning and modifying the [demo repository](https://github.com/hapi-server/server-python-demo).

Other examples:
* https://github.com/hapi-server/server-python-supermag
* https://github.com/rweigel/server-python-psws

# Install and Run Demo

```bash
git clone https://github.com/hapi-server/server-python-demo
cd server-python-demo
pip install -e .
hapiserver -h
hapiserver --config src/config.json
```
