# wf-4-tf-cf-buk-2-pub-2-big
**W**ork**F**low **for** **T**erra**f**orm, **C**loud **F**unction, **Bu**c**k**et **to** **Pub**/sub **to** **Big**Query

API/Tools/frameworks used
- **GITHUB actions** pipeline for CI/CD
- **Workload Identity Federation**: For authenticating the github actions pipeline to google project
- **Terraform**: For the creation of various resource like GCS Bucket, Cloud Function, PUB/SUB, BIGQuery etc.
- **Pub/Sub**: For storing the xml data
- **GCS Bucket**:
  1. For storing the terraform state.
  2. For storing the code for cloud function.
  3. For storing the source file from where the CF will pick the data.
  4. For storing the target file where CF will publish the data after processing the source file.
- **BigQuery**: For storing json formatted data.
- **Cloud function**: For the deployment of an API written in PYTHON.
  1. Which triggers from the gcs bucket.
  2. Picks the xml file from source bucket.
  3. Extracts the xml content from file.
  4. Publish each xml content into pub/sub topic.
  5. Creates xml file for each xml content and publish it to a target bucket.
  6. Converts each xml content into a json string.
  7. publish each json string to a BigQuert table.
