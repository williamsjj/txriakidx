#!/usr/bin/python
####################################################################
# FILENAME: test_riakidx.py
# PROJECT: Twisted Riak w/ Indexes
# DESCRIPTION: Tests for txRiakIdx errors
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

from twisted.trial import unittest
from txriakidx import errors

class ErrorsTestCase(unittest.TestCase):
    
    def test_txriakidx_error(self):
        "Validate txRiakIdxError."
        err = errors.txRiakIdxError("Test error.")
        self.assertEqual("Test error.", str(err))
    
    def test_illegal_datatype_error(self):
        "Validate IllegalDataTypeError."
        err = errors.IllegalDatatypeError("dict")
        self.assertEqual("Datatype dict not allowed in index " +
                         "definitions. Only: str, int, float, unicode", str(err))
    
    def test_general_index_error(self):
        "Validate IndexError."
        err = errors.IndexError("Test error.")
        self.assertEqual("Test error.", str(err))