$key = "d:\RAG-Agentic-Looping\pem\hostdare.pem"
$remote = "hyuser@10.15.0.29"
$cmd = "curl -s 'http://127.0.0.1:8081/api/v1/target-changes?kind=macro&scope=observed&limit=100' > /tmp/macro_obs.json; curl -s 'http://127.0.0.1:8081/api/v1/target-changes?kind=macro&limit=100' > /tmp/macro_changed.json; curl -s 'http://127.0.0.1:8081/api/v1/target-changes?kind=asset&limit=100' > /tmp/asset.json; wc -c /tmp/macro_obs.json /tmp/macro_changed.json /tmp/asset.json"
ssh -i $key -o BatchMode=yes $remote $cmd
scp -i $key -o BatchMode=yes "${remote}:/tmp/macro_obs.json" D:\RAG-Agentic-Looping\prod_macro_observed.json
scp -i $key -o BatchMode=yes "${remote}:/tmp/macro_changed.json" D:\RAG-Agentic-Looping\prod_macro_changed.json
scp -i $key -o BatchMode=yes "${remote}:/tmp/asset.json" D:\RAG-Agentic-Looping\prod_asset.json
"DONE"
