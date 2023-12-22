# DBSM

## Debugging DBSM

To debug DBSM, follow these steps:

1. Clone this repository within the `etendo_core` project if possible.

2. If you followed the recommendation in step 1, configure the path of `etendo_project` within gradle.properties as follows:

    ```
    etendo_project=..
    ```

3. Inside etendo_core:

   a. In `settings.gradle`, add:

      ```
      include ":etendo_dbsm"
      ```

   b. In `build.gradle`, within the dependencies block, add:

      ```
      dependencies {
         implementation project(':etendo_dbsm')
      }
      ```

4. Follow these debugging steps to set up DBSM for debugging within the `etendo_core` project.


## Publish new version of DBSM in Github Packages

The following steps describe the process for publishing a new version of DBSM to Github Packages

1. **Clone the Repository:**

   ```
   git clone <repository_url>
   ```

2. **Initialize Git Flow:**

   ```
   git flow init
   ```

3. **Start a Release:**

   ```
   git flow release start $newVersion
   ```

4. **Update Version in build.gradle:**

   Modify `'VERSION'` constant in the build.gradle file with the new version.

5. **Commit Version Update:**

    ```
    git add build.gradle
    git commit -m "Update version to $newVersion :zap:"  # Edit the message accordingly
    ```

6. **Finish Release:**

   ```
   git flow release finish $newVersion
   ```

7. **Create gradle.properties File:**

   Copy `gradle.properties.template` to a new file named `gradle.properties`. Indicate the location of the `etendo_core` project and provide Github credentials.

8. **Main Branch Checks:**

   If necessary, on the `main` branch, ensure that the deployment repository and version are not of snapshot type.

9.  **Build Project for JAR Generation**

    ```
    ./gradlew build
    ```

10. **Publish to Github Packages:**

    ```
    ./gradlew publish
    ```
