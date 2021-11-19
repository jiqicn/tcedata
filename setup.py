from setuptools import setup

setup(
    name = 'tcedata',
    packages = ['tcedata'],
    version = '1.0',
    license='MIT',
    description = 'Tools to download data from storages hosted by UvA-TCE to user workspace.',
    author = '@jiqicn',
    author_email = 'qiji1988ben@gmail.com',
    url = 'https://github.com/jiqicn/tcedata',
    download_url = 'https://github.com/jiqicn/tcedata',
    keywords = ['Storage', 'Jupyter', 'Widgets', "Minio"],
    install_requires=[
        'minio',
        'pandas',
        'ipywidgets'
        'ipyfilechooser', 
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable', 
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
    ],
)