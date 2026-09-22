import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {runExperiment} from './wind-tunnel.mjs';

export function verifyEvidence(artifact) {
  if(artifact?.format!=='marketpulse-evidence-v1'||artifact.algorithm!=='SHA-256'||!artifact.evidence)
    throw new Error('Unsupported evidence artifact');
  const checksum=createHash('sha256').update(JSON.stringify(artifact.evidence)).digest('hex');
  if(checksum!==artifact.sha256) throw new Error('Evidence checksum mismatch');
  const replay=runExperiment({snapshot:artifact.evidence.snapshot,...artifact.evidence.inputs});
  if(JSON.stringify(replay.evidence)!==JSON.stringify(artifact.evidence)) throw new Error('Replay differs from the recorded experiment');
  return {verified:true,sha256:checksum,scenario:replay.report.scenario,naive:replay.report.naive.state,guarded:replay.report.guarded.state};
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  try {
    if(!process.argv[2]) throw new Error('Usage: node verify-evidence.mjs path/to/evidence.json');
    const result=verifyEvidence(JSON.parse(await readFile(process.argv[2],'utf8')));
    console.log(JSON.stringify(result,null,2));
    console.log('Checksum and deterministic replay match. This is not a signed proof of data origin.');
  } catch(error) {console.error(error.message);process.exitCode=1;}
}
