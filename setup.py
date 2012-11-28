from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-datapreview',
	version=version,
	description="Provides data for the data preview from the resource cache",
	long_description="""\
	""",
	classifiers=[],
	keywords='',
	author='Ross Jones',
	author_email='ross@servercode.co.uk',
	url='',
	license='',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.datapreview'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
	    'xlrd',
	    'brewery'
	],
	entry_points=\
	"""
        [ckan.plugins]
	    datapreview=ckanext.datapreview.plugin:DataPreviewPlugin

        [paste.paster_command]
        prepresourcecache = ckanext.datapreview.command:PrepResourceCache

	""",
)
