# MIT License
#
# Copyright (c) 2018 Omidiora Samuel, samparsky@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from bitcoinetl.bitcoin_utils import bitcoin_to_satoshi
from bitcoinetl.domain.transaction_output import BtcTransactionOutput


class BtcTransactionOutputMapper(object):

    def json_dict_to_outputs(self, json_rpc_dict, pybitcointools_dict=None):
        outputs = []
        for item in json_rpc_dict.get('vout', []):
            output = self.json_dict_to_output(item)

            # Getting the value from the raw transaction is necessary as JSON RPC value has precision loss caused
            # by using float https://twitter.com/EvgeMedvedev/status/1076383275621306368
            # https://github.com/bitcoin/bitcoin/pull/3759
            value = self._get_value_by_output_index(pybitcointools_dict, output.index)
            if value is not None:
                output.value = value

            outputs.append(output)
        return outputs

    def json_dict_to_output(self, json_dict):
        output = BtcTransactionOutput()

        output.index = json_dict.get('n')
        output.addresses = json_dict.get('addresses')
        output.txinwitness = json_dict.get('txinwitness')
        output.sequence = json_dict.get('sequence')
        output.value = bitcoin_to_satoshi(json_dict.get('value'))
        if 'scriptPubKey' in json_dict:
            script_pub_key = json_dict.get('scriptPubKey')
            output.script_asm = script_pub_key.get('asm')
            output.script_hex = script_pub_key.get('hex')
            output.required_signatures = script_pub_key.get('reqSigs')
            output.type = script_pub_key.get('type')
            output.addresses = script_pub_key.get('addresses')

        return output

    def outputs_to_dicts(self, outputs):
        result = []
        for output in outputs:
            item = {
                'index': output.index,
                'script_asm': output.script_asm,
                'script_hex': output.script_hex,
                'required_signatures': output.required_signatures,
                'sequence': output.sequence,
                'type': output.type,
                'addresses': output.addresses,
                'value': output.value
            }
            result.append(item)
        return result

    def _get_value_by_output_index(self, pybitcointools_dict, output_index):
        if pybitcointools_dict is None:
            return None
        pybitcointools_outs = pybitcointools_dict.get('outs')

        if pybitcointools_outs is None or len(pybitcointools_outs) <= output_index:
            return None

        return pybitcointools_outs[output_index].get('value')
