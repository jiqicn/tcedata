"""
APIs of downloading pVol data from the Minio server

Author: @jiqicn
"""
import ipywidgets as widgets
import minio
import pandas as pd
from pathlib import Path
import os
import io


class Minio(object):
    """
    APIs for navigating and downloading data from the UvA-TCE Minio file server 
    """
    def __init__(self):
        self.mc = None  # minio client
        self.bucket = ""  # current bucket
        self.prefix = ""  # current folder
        self.selection = []  # selection of the file paths
        
    def login(self, endpoint, user_name, password):
        """
        login to the UvA-TCE ninio server
        """
        self.mc = minio.Minio(
            endpoint=endpoint, 
            access_key=user_name, 
            secret_key=password,
            region="nl", 
            secure=True
        )
        
        # check if successfully login
        try:
            self.mc.list_buckets()
        except Exception as e:
            print("! %s" % e.message)
            return
        print("* Login successfully!")
            
    def get_bucket_list(self):
        """
        get list of buckets that is accessable with the current permission
        """
        buckets = self.mc.list_buckets()
        bucket_names = []
        for b in buckets:
            bucket_names.append(b.name)
        return bucket_names
    
    def select_bucket(self, bucket):
        """
        select a bucket for further tasks
        """
        self.bucket = bucket
    
    def navigate(self, next_step=None):
        """
        navigate within the 
        
        next_step[str or None]: 
            if str, must be in format of "XXX/", where "XXX" is the directory name;
            if None, go back to a previous step; 
        """
        if next_step is None:
            temp = self.prefix.split("/")
            temp = temp[:len(temp)-2]
            self.prefix = "/".join(temp) + "/"
        else:
            if next_step in self.get_object_list():
                self.prefix += next_step
            else:
                print("! Target directory not exists.")
            
        if self.prefix == "/":
            self.prefix = ""
    
    def get_object_list(self):
        """
        get list of objects in a specific bucket and under a given directory
        
        -parameters-
        bucket[str]
        prefix[str]: path to target directory
        """
        if self.bucket == "":
            return []
        objects = self.mc.list_objects(
            bucket_name=self.bucket, 
            prefix=self.prefix
        )
        object_names = []
        for obj in objects:
            if obj.is_dir:
                obj_name = obj.object_name.split("/")[-2] + "/"
            else:
                obj_name = obj.object_name.split("/")[-1]
            object_names.append(obj_name)
        return object_names
    
    def save_selection(self, object_names):
        """
        accumulate the current selection
        
        -parameters-
        object_names[list]: list of file names for downloading
        """
        for obj_name in object_names:
            self.selection.append(self.prefix + obj_name)
        print("%d files saved to selection!" % len(object_names))
    
    def download_to_files(self, target_dir):
        """
        download the selected objects to a user selected folder
        
        -parameters-
        bucket[str]: name of the target bucket
        target_dir[str]
        """
        if target_dir is None:
            print("! Please choose the folder to keep your files.")
            return
        
        print("* Start downloading...")
        for obj in self.selection:
            fpath = os.path.join(target_dir, obj.split('/')[-1])
            try:
                self.mc.fget_object(
                    bucket_name=self.bucket, 
                    object_name=obj, 
                    file_path=fpath
                )
            except:
                print("! Fail to download %s." % obj)
                return
            
        print("* Download successfully to %s" % target_dir)