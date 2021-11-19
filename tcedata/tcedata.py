"""
GUIs of downloading data from various sources

Author: @jiqi
"""
from .minio import Minio

import ipywidgets as widgets
from ipyfilechooser import FileChooser


def get_files_from_minio():
    """
    GUI of pvol data downloading
    """
    mc = Minio()
    
    ######################
    # define GUI widgets #
    ######################
    w_endpoint = widgets.Text(
        value="fnwi-s0.science.uva.nl:9001",
        description="Endpoint"
    )
    w_user = widgets.Text(description="User Name")
    w_pwd = widgets.Password(description="Password")
    w_login = widgets.Button(description="Login")
    w_login_output = widgets.Output()
    w_target = FileChooser(
        title="Download data to: ", 
        show_only_dirs=True
    )
    w_bucket = widgets.Dropdown(description="Bucket")
    w_prefix = widgets.HTML()
    w_objects = widgets.SelectMultiple(description="Objects")
    w_back = widgets.Button(description="Back")
    w_download = widgets.Button(description="Download")
    w_download_output = widgets.Output()
    w_container = widgets.VBox([
        widgets.HTML('<b style="font-size: large">Minio @UvA-TCE</b>'), 
        w_endpoint, 
        w_user, 
        w_pwd, 
        w_login, 
        w_login_output
    ])
    
    ########################
    # define widget events #
    ########################
    def click_login(e=None):
        """
        login to the Minio server
        """
        w_login_output.clear_output()
        with w_login_output:
            mc.login(
                endpoint=w_endpoint.value, 
                user_name=w_user.value, 
                password=w_pwd.value
            )
            buckets = mc.get_bucket_list()
            buckets.insert(0, "")
            w_bucket.options = buckets
            w_container.children += (
                w_target, 
                widgets.HBox([
                    w_bucket,
                    w_prefix
                ]), 
                w_objects,
                widgets.HBox([
                    w_back, 
                    w_download
                ]), 
                w_download_output, 
            )
    w_login.on_click(click_login)
    
    def select_bucket(e=None):
        """
        change bucket to work with
        """
        mc.select_bucket(e["new"])
        w_objects.options = mc.get_object_list()
    w_bucket.observe(select_bucket, names='value')
    
    def select_object(e=None):
        """
        navigate or select files to download
        """
        selection = e["new"]
        if len(selection) == 1:
            selection = selection[0]
            if selection.endswith('/'):
                mc.navigate(selection)
                w_prefix.value = mc.prefix
                w_objects.options = mc.get_object_list()
    w_objects.observe(select_object, names="value")
    
    def click_back(e=None):
        """
        go back to the previous step
        """
        mc.navigate()
        w_prefix.value = mc.prefix
        w_objects.options = mc.get_object_list()
    w_back.on_click(click_back)
    
    def click_download(e=None):
        """
        download the selected objects as file
        """
        w_download_output.clear_output()
        with w_download_output:
            mc.download_to_files(
                object_names = w_objects.value, 
                target_dir = w_target.value
            )
    w_download.on_click(click_download)
    return w_container