from setuptools import setup, find_packages

setup(
    name='carmen',
    version='2.0.0',
    description='Geolocation for Twitter',
    author='',
    author_email='',
    url='',
    packages=find_packages(),
    package_data={'carmen': ['data/*']},
    install_requires=['geopy', 'jsonlines'],
    license='2-clause BSD',
    zip_safe=True
)
