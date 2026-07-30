uv is used to install packages only.
Packages are installed system-wide inside the container.

## Cloudflare Tunnel (public ingress)

The API machine (API + ClickHouse) sits behind a firewall and isn't publicly
reachable. A system-level `cloudflared` tunnel exposes the `api` container's
`localhost:9011` publicly so:

- the frontend can reach the API for every request, and
- the PROCESS machine can call `POST /admin/syncHourly` after rsyncing a
  date's Native-format export over (the rsync itself goes over a *separate*
  SSH tunnel/Cloudflare Access app under a different Cloudflare account — see
  `process/README.md` / `SSC/sync.py`'s `cloudflared access ssh` usage).

This tunnel runs as its own systemd service, independent of
`docker-compose.prod.api.yml` — so `docker compose down`/redeploys of the app
stack don't take down public ingress, and vice versa.

**Service name:** `cloudflared-api-oceaneco.service`

```bash
# start / stop
sudo systemctl start cloudflared-api-oceaneco
sudo systemctl stop cloudflared-api-oceaneco

# enable on boot (should already be enabled)
sudo systemctl enable cloudflared-api-oceaneco

# check status
sudo systemctl status cloudflared-api-oceaneco

# tail logs
journalctl -u cloudflared-api-oceaneco -f
```

If it's crash-looping, run the `ExecStart` command from
`/etc/systemd/system/cloudflared-api-oceaneco.service` directly in the
foreground as the service's user — systemd's journal often truncates the
actual `cloudflared` error above the restart-loop noise.