#!/usr/bin/python
####################################################################
# FILENAME: Makefile
# PROJECT: Twisted Riak w/ Indexes
# DESCRIPTION: Python package manifest
#
#
########################################################################################
# (C)2011 DigiTar, All Rights Reserved
# Distributed under the BSD License
# 
# Redistribution and use in source and binary forms, with or without modification, 
#    are permitted provided that the following conditions are met:
#
#        * Redistributions of source code must retain the above copyright notice, 
#          this list of conditions and the following disclaimer.
#        * Redistributions in binary form must reproduce the above copyright notice, 
#          this list of conditions and the following disclaimer in the documentation 
#          and/or other materials provided with the distribution.
#        * Neither the name of DigiTar nor the names of its contributors may be
#          used to endorse or promote products derived from this software without 
#          specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY 
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES 
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT 
# SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
# DAMAGE.
#
########################################################################################

from setuptools import setup, find_packages
 
version = '0.5.2'
 
setup(name='txRiakIdx',
      version=version,
      description="Twisted Riak client w/ transparent secondary indexes.",
      long_description="""Twisted Riak client w/ transparent secondary indexes.""",
      classifiers=[],
      keywords='twisted,riak,txriak,indexes',
      author='Jason Williams',
      author_email='jasonjwwilliams@gmail.com',
      url='https://github.com/williamsjj/txriakidx',
      download_url='https://github.com/williamsjj/txriakidx/zipball/v0.5.2',
      license='Apache 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests', 'old*']),
      zip_safe=False,
      install_requires=["Twisted>=10.0",
                        "txRiak>=0.3.2"]
    )