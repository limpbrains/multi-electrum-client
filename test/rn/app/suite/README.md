# On-device suite entries

One wrapper per file in `test/unit/**`. Metro can only serve bundle entries
from inside the app root, so each wrapper re-imports its real test file,
which Metro reaches via `watchFolders`. (`../suite-integration/` follows the
same pattern for `test/integration/**`, plus an `integration-env` import.)

Regenerate after adding/removing unit test files:

```bash
cd test/rn/app
rm -rf suite/*/ && for f in $(cd ../../unit && find . -name "*.test.ts" | sed 's|^\./||' | sort); do
  mkdir -p "suite/$(dirname "$f")"
  depth=$(echo "$f" | awk -F/ '{print NF}')
  up=""; for i in $(seq 1 $((depth+2))); do up="../$up"; done
  printf "// Auto-generated wrapper: Metro can only serve bundle entries from\n// inside the app root, so each on-device suite entry re-imports the real\n// test file from test/unit (reachable via watchFolders).\nimport '%sunit/%s';\n" "$up" "${f%.ts}" > "suite/$f"
done
```
