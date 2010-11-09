#! /usr/bin/python
#
# OFC resource to support running
# the LS-CAT Image Server "is"
# through pacemaker
#
# Copyright (C) 2009, 2010 by Keith Brister
# All rights reserved.
#

import sys
import os
import subprocess
import time

class ImageServer:
      
      def __init__( self):
          pass


      def getPid( self):
          pid = None
          try:
              f = open( "/var/run/ls-cat/is.pid", "r")
              pid = int(f.read())
              f.close()
          except:
              pass
          return pid
              

      def isRunning( self):
          pid = self.getPid()
          if pid == None:
              return False

          p = subprocess.Popen( "ps -p %d 2>/dev/null 1>/dev/null" % (pid), shell=True)
          sts = os.waitpid( p.pid, 0)
          return sts[1] == 0

      def validateAll( self):
          si = None
          try:
              si = os.stat( "/pf/bin/linux-x86_64/is")
          except:
              pass

          if si==None:
              return 5          # ocf_err_installed

          return 0              # ocf_success

      def metaData( self):
          print """
<?xml version="1.0"?>
<!DOCTYPE resource-agent SYSTEM "ra-api-1.dtd">
<resource-agent name="ImageServer">
<version>1.0</version>

<longdesc lang="en">
This resource controls the LS-CAT image server.
</longdesc>
<shortdesc lang="en">LS-CAT Image Server</shortdesc>

</resource-agent>
"""

      def monitor( self):
          if self.isRunning():
              return 0          # ocf_success

          return 7              # ocf_not_running

      def start( self):
          if self.isRunning():
              return 0          # ocf_success
          p = subprocess.Popen( "/pf/bin/linux-x86_64/is -d", shell=True)
          time.sleep( 2)
          if self.isRunning():
              return 0          # ocf_success
          else:
              return 1          # ocf_err_generic

      def stop( self):
          if not self.isRunning():
              return 0          # ocf_success

          pid = self.getPid()
          if pid == None:       # newly deceased?
              return 0          # ocf_success

          # Try the easy way
          p = subprocess.Popen( "kill %d" % (pid), shell=True)
          os.waitpid( p.pid, 0)  # we don't look at the status yet, kill can succeed without stopping the process
          time.sleep( 2)         # pause to allow things to settle
          
          if not self.isRunning():
              return 0      # ocf_success

          p = subprocess.Popen( "kill -9 %d" % (pid), shell=True)       # sure kill?
          os.waitpid( p.pid, 0)  # we don't look at the status yet, kill can succeed without stopping the process
          time.sleep( 2)         # pause to allow things to settle

          if not self.isRunning():
              return 0          # ocf_success

          return 1              # ocf_err_generic
          

      def run( self, argv):
          if len( argv) < 2:
              sys.exit( 3)      # ocf_err_unimplemented

          if argv[1] == "monitor":
              sys.exit( self.monitor())
          
          if argv[1] == "start":
              sys.exit( self.start())
          
          if argv[1] == "stop":
              sys.exit( self.stop())
          
          if argv[1] == "meta-data":
              sys.exit( self.metaData())
          
          if argv[1] == "validate-all":
              sys.exit( self.validateAll())
          

if __name__ == "__main__":
    ocfis = ImageServer()
    ocfis.run( sys.argv)
