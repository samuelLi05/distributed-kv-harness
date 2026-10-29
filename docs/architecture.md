# Architecture

## ABD replication

`abd` and `abd_basic` use RF=3 majority quorums. Writes query a majority for the maximum logical timestamp, then persist a higher-tagged value or tombstone on a majority. Reads select the largest returned tag. Basic mode confirms a second phase; fast mode returns when two durable replies agree on the complete tag/value, otherwise it repairs a majority. Every ingress node coordinates directly with the key's owners. Striped apply workers serialize replica updates until journal commits complete, including after caller cancellation.
