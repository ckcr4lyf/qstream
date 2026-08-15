#!/usr/bin/env python3
"""Fake UPnP IGD for testing qstream's upnp.rs (N4).

Answers SSDP M-SEARCH on 239.255.255.250:1900 with a LOCATION header
pointing at a local HTTP server serving the device description and
responding to GetExternalIPAddress + AddPortMapping SOAP calls.
Records what the client asked for, so tests can assert the mapping.

Usage: fake_igd.py [external_ip] [http_port]   (defaults 203.0.113.9 / 18099)
"""
import http.server
import socket
import socketserver
import sys
import threading

EXTERNAL_IP = sys.argv[1] if len(sys.argv) > 1 else "203.0.113.9"
HTTP_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 18099

DESCRIPTION = f"""<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType>
    <friendlyName>fake-igd</friendlyName>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
        <serviceId>urn:upnp-org:serviceId:WANIPConn1</serviceId>
        <controlURL>/ctl/IPConn</controlURL>
      </service>
    </serviceList>
  </device>
</root>"""

LOG = []


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _respond(self, body: bytes, ctype="text/xml"):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/desc"):
            self._respond(DESCRIPTION.encode())
        else:
            self._respond(b"not found", "text/plain")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode(errors="replace")
        action = None
        for line in body.splitlines():
            if line.strip().startswith("<u:"):
                action = line.strip().split(" ")[0][3:]
                break
        if not action:
            self._respond(b"<s:Envelope/>", "text/plain")
            return
        LOG.append(action)
        if action == "GetExternalIPAddress":
            reply = f"""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><u:GetExternalIPAddressResponse xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1">
<NewExternalIPAddress>{EXTERNAL_IP}</NewExternalIPAddress>
</u:GetExternalIPAddressResponse></s:Body></s:Envelope>"""
        elif action == "AddPortMapping":
            for line in body.splitlines():
                if "NewExternalPort" in line:
                    port = line.strip()
            reply = f"""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><u:AddPortMappingResponse xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1">
</u:AddPortMappingResponse></s:Body></s:Envelope>"""
        else:
            reply = "<s:Envelope/>"
        self._respond(reply.encode())


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def ssdp():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("", 1900))
    mreq = socket.inet_aton("239.255.255.250") + socket.inet_aton("0.0.0.0")
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    print(f"fake IGD: SSDP on 239.255.255.250:1900, HTTP on :{HTTP_PORT}, external {EXTERNAL_IP}", flush=True)
    while True:
        data, addr = s.recvfrom(4096)
        if b"M-SEARCH" in data:
            s.sendto(
                (
                    f"HTTP/1.1 200 OK\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n"
                    f"LOCATION: http://127.0.0.1:{HTTP_PORT}/desc.xml\r\n\r\n"
                ).encode(),
                addr,
            )


if __name__ == "__main__":
    httpd = ThreadedHTTPServer(("127.0.0.1", HTTP_PORT), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        ssdp()
    except KeyboardInterrupt:
        print("actions seen:", LOG)
