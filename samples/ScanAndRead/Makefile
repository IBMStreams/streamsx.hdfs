# begin_generated_IBM_copyright_prolog                             
#                                                                  
# This is an automatically generated copyright prolog.             
# After initializing,  DO NOT MODIFY OR MOVE                       
# **************************************************************** 
# THIS SAMPLE CODE IS PROVIDED ON AN "AS IS" BASIS. IBM MAKES NO   
# REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED, CONCERNING    
# USE OF THE SAMPLE CODE, OR THE COMPLETENESS OR ACCURACY OF THE   
# SAMPLE CODE. IBM DOES NOT WARRANT UNINTERRUPTED OR ERROR-FREE    
# OPERATION OF THIS SAMPLE CODE. IBM IS NOT RESPONSIBLE FOR THE    
# RESULTS OBTAINED FROM THE USE OF THE SAMPLE CODE OR ANY PORTION  
# OF THIS SAMPLE CODE.                                             
#                                                                  
# LIMITATION OF LIABILITY. IN NO EVENT WILL IBM BE LIABLE TO ANY   
# PARTY FOR ANY DIRECT, INDIRECT, SPECIAL OR OTHER CONSEQUENTIAL   
# DAMAGES FOR ANY USE OF THIS SAMPLE CODE, THE USE OF CODE FROM    
# THIS [ SAMPLE PACKAGE,] INCLUDING, WITHOUT LIMITATION, ANY LOST  
# PROFITS, BUSINESS INTERRUPTION, LOSS OF PROGRAMS OR OTHER DATA   
# ON YOUR INFORMATION HANDLING SYSTEM OR OTHERWISE.                
#                                                                  
# (C) Copyright IBM Corp. 2013-2014  All Rights reserved.         
#                                                                  
# end_generated_IBM_copyright_prolog      
                            
.PHONY: all clean

SPLC_FLAGS = -a -z -t ../../com.ibm.streamsx.hdfs

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE1 = hdfsexample::ScanAndRead
SPL_MAIN_COMPOSITE2 = hdfsexample::ScanAndReadWithInputPort
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed

all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2)
	-rm -rf toolkit.xml
	-rm -rf data/FileContents.txt
	-rm -rf data/FileContentsInputPort.txt	

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2)
	-rm -rf toolkit.xml
	-rm -rf data/FileContents.txt
	-rm -rf data/FileContentsInputPort.txt