# wf-4-tf-cf-buk-2-pub-2-big
Work Flow for Terraform, Cloud Function, Bucket to Pub/sub to BigQuery

* This code deploys a cloud function in GC using Terraform.
* This code uses github actions pipeline which uses WIF for authentication and initialize terraform.
* Terraform is used to create various buckets, pubsub topic, terraform state file and cloud function.

Cloud function works as follows:
- Cloud function reads the xml file from bucket_sourcecode_4_wf_4_tf_buk_2_pub_big
- extract the content inside each 'book' tag from the xml.
    sample xml:
    <?xml version="1.0"?>
    <catalog>
       <book id="bk101">
          <author>Corets, Eva</author>
       </book>
       <book id="bk102">
          <author>Corets, Eva</author>
       </book>
    </catalog>
- publishes each xml content with 'book' tag into pubsub.
    sample xml1:
      <book id="bk101">
          <author>Corets, Eva</author>
       </book>
    sample xml2:
      <book id="bk102">
          <author>Corets, Eva</author>
       </book>
- Creates an xml file for each book tag inside the source xml file and publishes it into a target bucket: bucket_targetfile_4_wf_4_tf_buk_2_pub_big.
