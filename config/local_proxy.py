from flask import Flask, request
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__)

# The remote OCI backend that python can reach but Chromium blocks
OCI_BACKEND = "https://129.153.166.6:443"

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy(path):
    url = f"{OCI_BACKEND}/{path}"
    # Forward the incoming browser request straight to OCI over python's working channel
    resp = requests.request(
        method=request.method,
        url=url,
        headers={k: v for k, v in request.headers if k.lower() != 'host'},
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
        verify=False
    )
    return (resp.content, resp.status_code, resp.headers.items())

if __name__ == '__main__':
    print("Local Proxy running on http://127.0.0.1:8080 -> Forwarding to OCI...")
    app.run(host='127.0.0.1', port=8080)
