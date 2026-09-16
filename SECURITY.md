# Security

## Reporting a vulnerability

Email **hello@vantion.co** with "Security" in the subject. Include a
description, the affected commit, and steps to reproduce. Please do not open a
public issue.

We aim to reply within a few working days, confirm the issue, agree a
disclosure date with you, and credit you in the release notes unless you
prefer not to be named.

## Scope

In scope: code in this repository, in particular the websocket handling, the
hub's fan-out and the JSON frames it writes.

Out of scope: vulnerabilities in dependencies (report those upstream), and
anything that needs a deployment choice this repository does not make. Dispatch
ships no authentication and no TLS termination: it is a demo server for a
public market feed, and putting it on the internet as-is is a deployment
decision, not a vulnerability in the code.
