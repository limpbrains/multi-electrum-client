# On-device suite entries

One wrapper per file in `test/unit/**` (minus the two node-`ws`-server-backed
files). Metro can only serve bundle entries from inside the app root, so each
wrapper re-imports its real test file, which Metro reaches via `watchFolders`.

Regenerate after adding/removing unit test files:

```bash
cd test/rn/app
rm -rf suite/*/ && for f in $(cd ../../unit && find . -name "*.test.ts" | sed 's|^\./||' | sort); do
  case "$f" in transport/ws.test.ts|client/electrum-client.ws.test.ts) continue;; esac
  mkdir -p "suite/$(dirname "$f")"
  depth=$(echo "$f" | awk -F/ '{print NF}')
  up=""; for i in $(seq 1 $((depth+2))); do up="../$up"; done
  printf "// Auto-generated wrapper: Metro can only serve bundle entries from\n// inside the app root, so each on-device suite entry re-imports the real\n// test file from test/unit (reachable via watchFolders).\nimport '%sunit/%s';\n" "$up" "${f%.ts}" > "suite/$f"
done
```
