How to apply
Safest manual way

Back up:

cp -a ~/.bashrc ~/.bashrc.bak.$(date +%Y%m%d-%H%M%S)


Apply one patch (pick the right one):

If you already have the # --- OpenShift Banner & Prompt ... --- section:

patch -p1 -d ~ < oc-banner-once-existing.patch


If you do not have that section yet:

patch -p1 -d ~ < oc-banner-once-add.patch


Reload:

source ~/.bashrc

One-command “smart” way

Put the script + both patch files in the same directory, then run:

bash ./apply-oc-banner-once.sh


(It will back up ~/.bashrc, detect which case you’re in, and apply the right patch.)

What you should see after this

Opening a new shell: you’ll get your [osv-prd] prompt prefix, but no banner spam

oc version: no banner

oc login --web: banner prints once right after login

oc config use-context othercluster: banner prints once

oc_pin_context ...: banner prints once (I updated that helper too)

Quick note about “clean .bashrc” portability

The “add” patch inserts the OpenShift block right after the standard line:

# export PATH="$HOME/bin:$PATH"


That’s present in the default Ubuntu/Debian-ish .bashrc skeleton (like the one your file is based on). If someone’s .bashrc is wildly different, the patch may not apply cleanly — 
but in that case, the OpenShift block in the patch can still be copied in verbatim.

Once your patch is in place you can edit your .bashrc manually and change the following section:
# Colors (banner_bg | prompt_fg)

to change the color of your banner and cli prefix depending on the name of the config context.
  
use oc config get-contexts to list the contexts you're currently logged into

use oc config rename-context to create simplified names for each context environment
example:
oc config rename-context 'default/api-ocp-byu-edu:6443/jjpetric@byu.edu' ocp

This will rename the very long name to 'ocp', which shortens your cli prompt as well as the banner and makes the change between contexts much easier when using the 
oc config use-context command.

example: 
instead of needing to type 'oc config use-context default/api-ocp-byu-edu:6443/jjpetric@byu.edu', you could simply type 'oc config use-context ocp' and it would work.





