# Copyright (C) 2020 Alteryx, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
#The GoogleTranslate tool will take in a column 
#
#This will take a selected field and the selected language and translate each record

from ayx_plugin_sdk.core import (
    InputConnectionBase,
    Plugin,
    ProviderBase,
    register_plugin,
    RecordPacket,
    FieldType,
)
from googletrans import Translator

class GoogleTranslate(Plugin):


    def __init__(self, provider: ProviderBase):
        """Construct the AyxRecordProcessor."""
        self.config = provider.tool_config
        self.name = "GoogleTranslate"
        self.provider = provider
        self.output_anchor = self.provider.get_output_anchor("Output")

        self.provider.io.info(f"{self.name} tool started")

    def on_input_connection_opened(self, input_connection: InputConnectionBase):
        """Initialize the Input Connections of this plugin."""
        if input_connection.metadata is None:
            raise RuntimeError("Metadata must be set before setting containers.")

        new_field_name = self.provider.tool_config["TextBox"]

        output_metadata = input_connection.metadata.clone()
        output_metadata.add_field(
            name= new_field_name, field_type=FieldType.v_wstring, size=250
        )
        self.output_anchor.open(output_metadata)

    def on_record_packet(self, input_connection: InputConnectionBase):
        """Handle the record packet received through the input connection."""
        from pandas import DataFrame
        import pandas as pd
        selected_field = self.provider.tool_config["Field"]
        translation_dest = self.provider.tool_config["DropDownStringSelector1"]
        new_field_name = self.provider.tool_config["TextBox"]

        packet = input_connection.read()

        input_df = packet.to_dataframe()
        self.provider.io.info(str(input_df))
        output_df = input_df.copy()

        translator = Translator()
        translations = []
        #this code could be used for pulling in the dest per record based on a column
        #for lang in input_df.iloc[:,1]:
            #self.provider.io.info(lang)

        #loop through each record and translate it into the selected language from config    
        for x in input_df.loc[:, selected_field]:
            text1 = str(translator.translate(x, dest=translation_dest))
            listed_output = list(text1.split(','))
            text_output = str(listed_output[2])[6:]
            translations.append(text_output)
        
        #transform the list of translations into a temp df for replacement into the output df
        temp = DataFrame(translations)
        
        output_df[new_field_name] = temp
        self.provider.io.info(str(output_df))

        output_packet = RecordPacket.from_dataframe(
            self.output_anchor.metadata, output_df
        )

        self.output_anchor.write(output_packet)

    def on_complete(self) -> None:
        """Handle for when the plugin is complete."""
        self.provider.io.info(f"{self.name} tool done")


AyxPlugin = register_plugin(GoogleTranslate)
