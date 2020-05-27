# coding: utf-8
# Copyright (c) 2016, 2020, Oracle and/or its affiliates.  All rights reserved.
# This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown 
# at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown 
# at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

import filecmp
import oci
from oci.object_storage.models import CreateBucketDetails
import os
import sys


def upload_object(object_stor_client, namespace, bucket_name, object_name, data):
    print("Uploading new object {!r}".format(object_name))
    obj = object_stor_client.put_object(
        namespace,
        bucket_name,
        object_name,
        data)


def retrieve_object(object_stor_client, namespace, bucket_name, object_name):
    print("Retrieving object {!r}".format(object_name))
    retrieved_obj = object_stor_client.get_object(
        namespace,
        bucket_name,
        object_name)
    return retrieved_obj


def show_objects(object_stor_client, namespace, bucket_name):
    # List Objects in Bucket
    print("\nList Objects in Bucket {!r}".format(bucket_name))
    list_obj = object_stor_client.list_objects(namespace, bucket_name, 
        fields = "name,size,timeCreated")
    for item in list_obj.data.objects:
        print("Object <{}> with size <{}> created on <{}>".format(
            item.name, str(item.size), item.time_created))
    return list_obj.data.objects


def clean_up(object_stor_client, namespace, bucket_name, *args):
    #Delete Objects and Bucket
    try:
        for object_name in args:
            print("\nDeleting object {}".format(object_name))
            object_stor_client.delete_object(namespace, bucket_name, object_name)
    except oci.exceptions.ServiceError as e:
            print(e)

    finally:
        print("Deleting bucket {}".format(bucket_name))
        object_stor_client.delete_bucket(namespace, bucket_name)


if __name__ == "__main__":

    if (len(sys.argv) != 2) or not os.path.exists(sys.argv[1]):
        print('Invalid number of arguments provided.\n' + \
            'Usage: object_crud_demo.py <file_name_to_upload>')
        sys.exit()

    config = oci.config.from_file(file_location="./config.local")

    # Using root compartment
    compartment_id = config["tenancy"]
    object_storage = oci.object_storage.ObjectStorageClient(config)

    # Get Namespace and give a name to the Bucket
    namespace = object_storage.get_namespace().data
    bucket_name = "python-sdk-example-bucket"

    data_object_name = "python-sdk-example-object"
    example_file_object_name = 'example_file_object'

    try:
        # Create Bucket
        print("Creating a new bucket {!r} in compartment {!r} with namespace {!r}".format(
            bucket_name, compartment_id, namespace))
        request = CreateBucketDetails()
        request.compartment_id = compartment_id
        request.name = bucket_name
        bucket = object_storage.create_bucket(namespace, request)

        # Upload String content to OCI Object Storage
        my_data = 3*b"Hello, World!"
        upload_object(object_storage, namespace, bucket_name, data_object_name, my_data)

        # Upload local File provided as an argument to OCI Object Storage
        file_name = sys.argv[1]
        with open(file_name, "rb") as f:
            upload_object(object_storage, namespace, bucket_name, example_file_object_name, f)

        # List Object in OCI Object Storage root compartment
        show_objects(object_storage, namespace, bucket_name)

        # Retrieve Object uploaded as String Data
        retrieved_data = retrieve_object(object_storage, namespace, bucket_name, 
            data_object_name)

        #Check retrieved data and original data are a equal
        print("{!r} == {!r}: {}".format(
            my_data, retrieved_data.data.content,
            my_data == retrieved_data.data.content))

        # Retrieve the file, streaming it into another file in 1 MiB chunks
        print('\nRetrieving file from object storage')
        retrieved_file = retrieve_object(object_storage, namespace, bucket_name, 
            example_file_object_name)
        # Retrieved File has the string .retrieved appended to its name
        with open((file_name + ".retrieved"), 'wb') as f:
            for chunk in retrieved_file.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)

        print('\nUploaded and downloaded files are the same: {}'.format(
            filecmp.cmp(file_name, (file_name + '.retrieved'))))

    except oci.exceptions.ServiceError as e:
        print(e)


    finally:
        #Pause to check OCI
        input("\nPause to check OCI before cleaning up. Type any key to continue>")

        # Delete Bucket and Objects
        clean_up(object_storage, namespace, bucket_name, data_object_name, 
            example_file_object_name)

        if os.path.exists(file_name + '.retrieved'):
            print("Deleting downloaded File {} ".format(file_name + '.retrieved'))
            os.remove(file_name + '.retrieved')
