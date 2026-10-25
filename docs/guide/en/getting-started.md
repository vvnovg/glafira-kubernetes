# Getting started

Glafira for Kubernetes is a desktop app for working with Kubernetes clusters and diagnosing them, with OpenShift and
Platform V DropApp in focus. It connects with your own credentials and shows only what your rights allow. Nothing in
the cluster changes until you confirm it.

## Installation {#install}

Installers for macOS (`.dmg`), Windows (`.msi`) and Linux (`.deb`, `.rpm`, `.AppImage`) are published at
https://github.com/vvnovg/glafira-kubernetes-releases/releases. Your organization may configure the app with a policy:
some settings are then set by the administrator.

## First connection {#first-connection}

1. Click [[ui:sidebar.newConnection]] in the left panel. If you already use `oc` or `kubectl`, choose
   [[ui:sidebar.importKubeconfig]] instead: the contexts from your kubeconfig become connections.
2. Enter [[ui:connections.form.apiUrl]] — the address you pass to `oc login` or the `server` of a kubeconfig cluster,
   usually `https://api.<cluster>:6443`.
3. Choose [[ui:connections.form.authMethod]]: browser (OpenShift), token, client certificate or exec plugin. See
   [Sign-in methods](connections.md#sign-in).
4. Click [[ui:connections.form.test]]: Glafira checks the address, TLS, sign-in, platform and projects step by step and
   shows which step failed. Then click [[ui:common.save]].
5. The connection tab opens: it shows who you are signed in as, the platform, the Kubernetes version and what you can
   read at the cluster level.

## Interface language {#language}

Open [[ui:sidebar.settings]] and choose [[ui:settings.language.label]]. By default the app follows the system language.
If the language is set by your organization, the list is not available.

## Help {#help}

Press F1 on any screen to open the chapter about it. The guide works offline; use the search field at the top of
the contents.
