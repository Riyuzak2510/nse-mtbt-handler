#!/usr/bin/env python3
"""Compare publisher admission correctness; never infer matching throughput."""
import argparse,json
from pathlib import Path

def compare(async_dir,sync_dir):
 a=json.loads((async_dir/'diagnostic-analysis.json').read_text());s=json.loads((sync_dir/'diagnostic-analysis.json').read_text())
 pipeline=[json.loads(x)for x in(async_dir/'pipeline.jsonl').read_text().splitlines()][-1]
 readback=json.loads((async_dir/'ordered-readback.json').read_text())
 settings_a=json.loads((async_dir/'settings.json').read_text());settings_s=json.loads((sync_dir/'settings.json').read_text())
 same_image=settings_a['image_id']==settings_s['image_id']
 errors=[]
 if not same_image:errors.append('differentimage')
 if not a['reconciliation_pass']or not s['reconciliation_pass']:errors.append('packetreconciliationfailed')
 if a['source_events']!=s['source_events']:errors.append('differentsourcecounts')
 if a['socket_drop_delta']or a['missing_sequences']:errors.append('asynclossobserved')
 expected=a['source_including_heartbeat']
 if not all(pipeline[k]==expected for k in ('received','submitted','confirmed','last_confirmed_broker')):errors.append('pipelinecountmismatch')
 if any(pipeline[k]for k in ('unconfirmed','overflow','ack_errors','queued','in_flight','owned_bytes')):errors.append('pipelineundrainedorfailed')
 if readback['verified_messages']!=expected or not readback['exact_payload_and_envelope']or readback['redeliveries']:errors.append('exactreadbackfailed')
 outcome_s=json.loads((sync_dir/'diagnostic-outcome.json').read_text());outcome_a=json.loads((async_dir/'diagnostic-outcome.json').read_text())
 return dict(scope='finitepublisher-onlyadmissioncomparison;not20M/matchingcapacity',image_equal=same_image,
             ingestion_acceptance_pass=not errors,errors=errors,source_events=a['source_events'],
             async_published=a['published'],async_socket_drops=a['socket_drop_delta'],
             sync_published=s['published'],sync_socket_drops=s['socket_drop_delta'],
             sync_harness_reason=outcome_s['reason'],async_harness_reason=outcome_a['reason'],
             async_pipeline=pipeline,exact_readback=readback,
             source_to_receive={'async':a['emitted_to_receive_ms'],'sync':s['emitted_to_receive_ms']},
             publish_observation={'async_submit_to_ordered_ack_retirement':a['publish_client_latency_ms'],
                                  'sync_publish_call_wait':s['publish_client_latency_ms']},
             limitations=['SequentialsharedVMruns/cache/background/schedulingdiffer','Synchronousrunflaggedenvironmentinterruptionaftercompletegenerator;source/drop/brokercountsstillreconcilebutnotcleanlatencymatchedcomparison','AsyncorderedackretirementisnotpurewireRTTorcomparabletosyncwaitwithoutboundarycaveat','NoDBconsumerineitheradmissiontest;brokerpending161001isexpectednotmatchingthroughput','Finite10k/speakisnot20Msoakorcapacity/SLAguarantee'])

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('async_dir',type=Path);p.add_argument('sync_dir',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 d=compare(a.async_dir,a.sync_dir);a.output.write_text(json.dumps(d,indent=2)+'\n');print(json.dumps({k:v for k,v in d.items()if k not in ('source_to_receive','publish_observation')},indent=2))
if __name__=='__main__':main()
