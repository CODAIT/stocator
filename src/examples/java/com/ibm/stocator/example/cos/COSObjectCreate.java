package com.ibm.stocator.example.cos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class COSObjectCreate {

  private static Configuration mConf = new Configuration(true);

  public static void main(String[] args) {

    // global Stocator-COS definitions
    mConf.set("fs.stocator.scheme.list", "cos");
    mConf.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem");
    mConf.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient");
    mConf.set("fs.stocator.cos.scheme", "cos");

    // "us-geo" is the service name. Can be any other name
    // All configuration keys will have the prefix fs.cos.us-geo

    //Usage with access key and secret key
    mConf.set("fs.cos.us-geo.endpoint", "http://s3-api.us-geo.objectstorage.softlayer.net");
    mConf.set("fs.cos.us-geo.v2.signer.type", "false");
    mConf.set("fs.cos.us-geo.access.key", "ACCESS KEY");
    mConf.set("fs.cos.us-geo.secret.key", "SECRET KEY");

    //Usage with IAM service
    /*
    mConf.set("fs.cos.us-geo.endpoint", "http://s3-api.us-geo.objectstorage.softlayer.net");
    mConf.set("fs.cos.us-geo.v2.signer.type", "false");
    mConf.set("fs.cos.us-geo.iam.api.key", "API KEY");
    mConf.set("fs.cos.us-geo.iam.service.id", "SERVICE ID");
     */

    FileSystem fs = null;
    // gvernik is the bucket name
    String path = "cos://gvernik.us-geo/mydata/data.txt";
    try {

      fs = FileSystem.get(URI.create(path), mConf);
      System.out.println("Going to create a new object: " + path);
      FSDataOutputStream out = fs.create(new Path(path));
      String data = "The content of my new object";
      out.write(data.getBytes());
      out.close();
      System.out.println("New object was created. Let's read it back");
      FSDataInputStream in =  fs.open(new Path(path));
      byte[] b = new byte[data.length()];
      in.readFully(0, b);
      in.close();
      System.out.println(new String(b));

    } catch (Exception e) {
      System.out.println("Not suppose to happen ;) Got an error!");
      e.printStackTrace();
    }
  }
}
