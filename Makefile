# Convenience targets. Everything runs locally against ./lakehouse (gitignored).
.PHONY: test demo daily live backfill-dry lineage skew-lab jar fmt clean dag-test

test:            ## unit + integration tests
	sbt test

demo:            ## scripted 30-day season end to end
	sbt "run demo"

daily:           ## one incremental run over whatever is in landing/
	sbt "run daily"

live:            ## pull real data from api.deadlock-api.com, then run
	sbt "run daily --api"

backfill-dry:    ## show what a backfill from dim_item would rebuild
	sbt "run backfill --start-at gold.dim_item --from 2026-10-01 --to 2026-10-31 --dry-run"

lineage:         ## print the lineage diagram (Mermaid)
	sbt -error "run lineage"

skew-lab:        ## join-skew experiment
	sbt "run skew-lab"

jar:             ## fat jar for spark-submit
	sbt assembly

fmt:
	sbt scalafmtAll

dag-test:
	pytest -q airflow/tests

clean:
	rm -rf lakehouse target project/target
