# Security policy

## Supported versions

Only the latest release on https://github.com/vvnovg/glafira-kubernetes-releases/releases receives fixes.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting: on the repository page open
the **Security** tab and click **Report a vulnerability**. Describe the problem, the app version and the steps
to reproduce it.

You will get a reply within 7 days. When a fix is released, the report is published as a security advisory with
credit to the reporter, unless you ask otherwise.

## What matters most

Glafira connects to Kubernetes clusters, OpenShift and Platform V DropApp among them, with the user's credentials
(tokens, client certificates, browser login, exec plugins). Reports about credential handling (system secret store,
logs), the OAuth login flow, exec plugins and the administrator policy are especially welcome.
