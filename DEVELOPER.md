Developer FAQ
=============

* github actions dont trigger for my PR  
  if you modified it, make sure .github/workflows/pr.yaml is valid yaml. 
  you will get no warnings or clues if its malformed
* my build fails on recent JDKs (16, for example)
  the version of gradle we use (6.*) does not support the most recent JDKs. use 11.
* in unable to check out the project from git on windows - some paths are too large
  set core.longpaths = true in C:\Program Files\Git\etc\gitconfig
* fastserde does not build on windows
  talk to felix