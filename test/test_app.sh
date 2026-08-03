# Usage: test_app.sh <gunicorn|uvicorn>

# Usage: test_app.sh <gunicorn|uvicorn> <demo_dir>
# demo_dir is the path to a checkout of server-python-demo (see demo_repo.py).

server=$1
demo_dir=$2

port=8675
if [ "$server" != "gunicorn" ] && [ "$server" != "uvicorn" ] || [ -z "$demo_dir" ]; then
  echo "Usage: $0 <gunicorn|uvicorn> <demo_dir>"
  exit 1
fi

cd "$demo_dir"

overall_result=0

for METHOD in 1 2 3 4; do
  export METHOD
  echo -e "\nMETHOD=$METHOD, server=$server\n"
  if [ "$server" == "gunicorn" ]; then
    gunicorn src.app:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$port --workers 2 &
  else
    uvicorn src.app:app --host 0.0.0.0 --port $port --workers 2 &
  fi
  pid=$!

  # Poll until the server responds instead of using a fixed sleep, since
  # startup time (worker spawn, module imports) can vary between runs.
  ready=0
  for i in $(seq 1 30); do
    if curl -s -o /dev/null http://localhost:$port/hapi/catalog; then
      ready=1
      break
    fi
    sleep 1
  done

  if [ $ready -ne 1 ]; then
    echo "METHOD=$METHOD, server=$server: server did not become ready"
  fi

  curl -s http://localhost:$port/hapi/catalog | grep '"code": 1200'
  result=$?

  kill $pid
  wait $pid 2>/dev/null

  if [ $result -ne 0 ]; then
    echo "METHOD=$METHOD, server=$server failed"
    overall_result=1
  fi
done

exit $overall_result
