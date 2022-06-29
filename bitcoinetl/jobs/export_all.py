# MIT License
#
# Copyright (c) 2018 Omidiora Samuel, Evgeny Medvedev, evge.medvedev@gmail.com, samparsky@gmail.com
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

import logging
import os
import sys
from time import time

import pyarrow as pa

from bitcoinetl.jobs.export_blocks_job import ExportBlocksJob
from bitcoinetl.jobs.enrich_transactions import EnrichTransactionsJob
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc
from blockchainetl.logging_utils import logging_basic_config
from blockchainetl.thread_local_proxy import ThreadLocalProxy

logging_basic_config()
logger = logging.getLogger('export_all')


def export_all(chain, partitions, output_dir, provider_uri, max_workers, batch_size, enrich):
    for batch_start_block, batch_end_block, partition_dir, *args in partitions:
        # # # start # # #

        start_time = time()

        padded_batch_start_block = str(batch_start_block).zfill(8)
        padded_batch_end_block = str(batch_end_block).zfill(8)
        block_range = '{padded_batch_start_block}-{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )
        inmemory_exporter = InMemoryItemExporter(item_types=[
            'block', 'transaction'])

        # # # blocks_and_transactions # # #

        blocks_output_dir = '{output_dir}/blocks{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )
        os.makedirs(os.path.dirname(blocks_output_dir), exist_ok=True)

        transactions_output_dir = '{output_dir}/transactions{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )
        os.makedirs(os.path.dirname(transactions_output_dir), exist_ok=True)

        logger.info('Exporting blocks {block_range}'.format(
            block_range=block_range,
        ))
        logger.info('Exporting transactions from blocks {block_range}'.format(
            block_range=block_range,
        ))

        job = ExportBlocksJob(
            chain=chain,
            start_block=batch_start_block,
            end_block=batch_end_block,
            batch_size=batch_size,
            bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
            max_workers=max_workers,
            item_exporter=inmemory_exporter,
            export_blocks=True,
            export_transactions=True)
        job.run()

        if enrich == True:
            job = EnrichTransactionsJob(
                transactions_iterable = inmemory_exporter.get_items('transaction'),
                batch_size = batch_size,
                bitcoin_rpc = ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
                max_workers = max_workers,
                item_exporter = inmemory_exporter,
                chain = chain
            )
            job.run()


        def get_list_array(arr):
            offsets = pa.array([0, len(arr)])
            return pa.ListArray.from_arrays(offsets, arr)

        block_array = pa.array(inmemory_exporter.get_items('block'), block_arrow_struct_type())
        transaction_array = pa.array(inmemory_exporter.get_items('transaction'), transaction_arrow_struct_type())

        block_list_array = get_list_array(block_array)
        transaction_list_array = get_list_array(transaction_array)

        combined_table = pa.Table.from_arrays([block_list_array, transaction_list_array], names=['blocks', 'transactions'])

        with pa.ipc.new_stream(sys.stdout.buffer, combined_table.schema) as writer:
            writer.write_table(combined_table)

        # # # finish # # #
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=block_range,
            time_diff=time_diff,
        ))

def block_arrow_struct_type() -> pa.DataType:
    return pa.struct([
        ('hash', pa.utf8()),
		("size", pa.int64()),
		("stripped_size", pa.int64()),
		("weight", pa.int64()),
		("number", pa.int64()),
		("version", pa.int64()),
		("merkle_root", pa.utf8()),
		("timestamp", pa.timestamp("us")),
		("nonce", pa.utf8()),
		("bits", pa.utf8()),
		("coinbase_param", pa.utf8()),
		("transaction_count", pa.int64()),
    ])

def transaction_arrow_struct_type() -> pa.DataType:
    return pa.struct([
        ("hash", pa.utf8()),
		("size", pa.int64()),
		("virtual_size", pa.int64()),
		("version", pa.int64()),
		("lock_time", pa.int64()),
		("block_hash", pa.utf8()),
		("block_number", pa.int64()),
		("block_timestamp", pa.timestamp("us")),
		("input_count", pa.int64()),
		("output_count", pa.int64()),
		("input_value", pa.decimal128(precision=38, scale=9)),
		("output_value", pa.decimal128(precision=38, scale=9)),
		("is_coinbase", pa.bool_()),
		("fee", pa.decimal128(precision=38, scale=9)),
        ("inputs", pa.list_(pa.struct([
            ("index", pa.int64()),
            ("spent_transaction_hash", pa.utf8()),
            ("spent_output_index", pa.int64()),
            ("script_asm", pa.utf8()),
            ("script_hex", pa.utf8()),
            ("sequence", pa.int64()),
            ("required_signatures", pa.int64()),
            ("addresses", pa.list_(pa.utf8())),
            ("value", pa.decimal128(precision=38, scale=9)),
        ]))),
        ("outputs", pa.list_(pa.struct([
            ("index", pa.int64()),
            ("script_asm", pa.utf8()),
            ("script_hex", pa.utf8()),
            ("required_signatures", pa.int64()),
            ("type", pa.utf8()),
            ("addresses", pa.list_(pa.utf8())),
            ("value", pa.decimal128(precision=38, scale=9)),
        ]))),
    ])