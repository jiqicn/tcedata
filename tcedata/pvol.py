"""
Classes developed for special purposes that is not directly related to the
core functionality of ipymeteovis
"""
import ipywidgets as widgets
import minio
import pandas as pd
from pathlib import Path
import io

DOWNLOAD_DIR = "./download"

# Access of a public MinIO server
EXAMPLE_MINIO_CLIENT = {
    "endpoint": "145.100.57.179:9000", 
    "username": "minioadmin", 
    "password": "minioadmin", 
    "secure": False
}


class Minio:
    """
    Remote access of Minio S3 server
    """

    def __init__(self, profile=None):
        self.client = None
        self.selection = []  # list of files to be streamed to local 
        self.bucket = ""
        self.prefix = ""
        self.profile = profile
        
        if profile is not None:
            self.client = minio.Minio(
                endpoint=profile["endpoint"],
                access_key=profile["username"],
                secret_key=profile["password"],
                region="nl",
                secure=True,
            )
            self.bucket = profile["bucket"]

    def login(self):
        """Login interface

        With the login interface, user can input their username and password to 
        get access to the UvA-TCE Minio server.
        """
        # widget: input endpoint
        w_endpoint = widgets.Text(
            value="fnwi-s0.science.uva.nl:9001",
#             value=EXAMPLE_MINIO_CLIENT["endpoint"],  
            description="End Point:",
        )
        
        # widget: input access key
        w_access_key = widgets.Text(
#             value=EXAMPLE_MINIO_CLIENT["username"],  
            description="Username:"
        )
        
        # widget: input secret key
        w_secret_key = widgets.Password(
#             value=EXAMPLE_MINIO_CLIENT["password"],  
            description="Password:"
        )
        
        # widget: login button
        w_login = widgets.Button(
            description="Login",
            icon="check"
        )
        def click_login(b):
            # initialise a Minio Client object
            self.client = minio.Minio(
                endpoint=w_endpoint.value,
                access_key=w_access_key.value,
                secret_key=w_secret_key.value,
                region="nl",
                secure=True
#                 secure=EXAMPLE_MINIO_CLIENT["secure"],
            )
            
            self.profile = {
                "endpoint": w_endpoint.value,
                "username": w_access_key.value,
                "password": w_secret_key.value,
                "bucket": ""
            }
            
            # get list of bucket and show the chooser
            try:
                buckets = self.client.list_buckets()
            except:
                with w_output:
                    log("Connection failed.")
                return
            w_bucket.options = [b.name for b in buckets]
            
            # get list of object under the current bucket and show the list
            objects = self.client.list_objects(w_bucket.value)
            w_objects.options = [
                obj._object_name.split("/")[-2] + "/"
                if obj.is_dir else obj._object_name.split("/")[-1]
                for obj in objects]
            
            # add the newly appeared widgets
            w_container.children += (
                widgets.VBox([
                    w_bucket, 
                    w_route, 
                    w_objects, 
                    widgets.HBox([w_back, w_select,])
                ]), 
            )
            with w_output:
                log("Login succesfully, please choose a bucket...")
            
        w_login.on_click(click_login)
        
        # widget: select bucket
        w_bucket = widgets.widgets.Dropdown(
            options=[],
            description='Choose a bucket:',
            disabled=False,
            style={'description_width': 'initial'},
        )
        def change_bucket(cb):
            self.bucket = w_bucket.value
            self.profile["bucket"] = w_bucket.value
            self.prefix = ""
            w_route.value = "Route: /" + self.prefix
            objects = self.client.list_objects(w_bucket.value)
            w_objects.options = [
                obj._object_name.split("/")[-2] + "/"
                if obj.is_dir else obj._object_name.split("/")[-1]
                for obj in objects]
        w_bucket.observe(change_bucket, names="value")
        
        # widget: show the current route
        w_route = widgets.HTML("Route: /")
        
        # widget: list objects
        w_objects = widgets.SelectMultiple(options=[])
        def change_selected_objects(cso):
            values = w_objects.value
            if len(values) == 1:
                value = values[0]
                # if a directory is selected, go to the next level
                if value.endswith("/"):
                    self.prefix += value
                    w_route.value = "Route: /" + self.prefix
                    objects = self.client.list_objects(
                        w_bucket.value, prefix=self.prefix
                    )
                    w_objects.options = [
                        obj._object_name.split("/")[-2] + "/"
                        if obj.is_dir else obj._object_name.split("/")[-1]
                        for obj in objects]
        w_objects.observe(change_selected_objects, names="value")
        
        # widget: go back
        w_back = widgets.Button(
            description="Back"
        )
        def click_back(cb):
            temp = self.prefix.split("/")
            temp = temp[:len(temp) - 2]
            self.prefix = "/".join(temp)
            if len(self.prefix) > 0: self.prefix += "/"
            w_route.value = "Route: /" + self.prefix
            objects = self.client.list_objects(
                w_bucket.value, prefix=self.prefix)
            w_objects.options = [
                obj._object_name.split("/")[-2] + "/"
                if obj.is_dir else obj._object_name.split("/")[-1]
                for obj in objects]
        w_back.on_click(click_back)
        
        # widget: submit selection of objects
        w_select = widgets.Button(
            description="Select"
        )
        def click_select(cs):
            self.selection = []
            with w_output:
                log("Start selecting files, please wait...")
            for obj in w_objects.value:
                if obj.endswith("/"):
                    files = self.client.list_objects(
                        w_bucket.value, prefix=self.prefix + obj)
                    self.selection += [f._object_name for f in files]
                else:
                    self.selection.append(self.prefix + obj)
            with w_output:
                log(str(len(self.selection)) + " files are selected.")
                
        w_select.on_click(click_select)

        # widget: container of the controls
        w_container = widgets.VBox([
            widgets.VBox([w_endpoint, w_access_key, w_secret_key, w_login]), 
        ], layout=widgets.Layout(width="50%"))
        
        # widdget: output on the right side
        w_output = widgets.Output()
        
        return widgets.VBox([
            widgets.HTML("<b style='font-size: large'>UvA FNWI Minio: Login</b>"),
            widgets.HBox([
                w_container, 
                widgets.VBox([
                    widgets.HTML("<b>Logs</b>"), w_output
                ], layout=widgets.Layout(width="50%"))
            ])
        ])
    
    def stream(self, file):
        """
        Stream object to memory
        
        return: BytesIO object that can be read in memory
        """
        try:
            data = self.client.get_object(self.bucket, file)
            return io.BytesIO(data.read())
        except:
            print(file)
    
    def get_profile(self):
        return self.profile