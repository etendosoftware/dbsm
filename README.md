# How to deploy a new version of DBSM to Nexus repository.

- Clone the repo
- `git flow init`
- `git flow release start $newVersion`
- Modify `build.gradle` with the new version
- `git add build.gradle && git commit -m "$newVersion message"` (Edit the message accordingly)
- `git flow release finish $newVersion -m $newVersion`
- If necessary, modifiy `settings.gradle` to indicate where the EtendoCore project is located.
- If necessary, on master branch, check that the deployment repo and version are not of snapshot type.
- `./gradlew build`
- `./gradlew :publish`