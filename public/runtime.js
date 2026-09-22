import config from './runtime-config.js';
if (!['api','public'].includes(config.mode)) throw new Error('Unsupported data mode');
export const publicMode = config.mode === 'public';
let providerPromise, downloadUrl;
async function provider() {
  providerPromise ??= Promise.all([import('./browser-provider.js'),import('./shared/analytics.mjs'),import('./shared/wind-tunnel.mjs')]).then(([module,analytics,engine])=>module.createBrowserProvider({analytics,runExperiment:engine.runExperiment}));
  return providerPromise;
}
function apiUrl(path) {
  if (!path.startsWith('/api/')) throw new Error('Unsupported API path');
  return config.apiBase ? `${config.apiBase.replace(/\/$/,'')}${path}` : path;
}
export async function request(path) {
  if(publicMode) return (await provider()).request(path);
  const response=await fetch(apiUrl(path),{cache:'no-store',credentials:'omit',signal:AbortSignal.timeout(12000)});
  const body=await response.json();
  if(!response.ok) throw new Error(body.error||`Request failed (${response.status})`);
  return body;
}
export async function evidenceUrl(experimentId) {
  if(!publicMode) return apiUrl(`/api/lab/evidence?experimentId=${encodeURIComponent(experimentId)}`);
  const artifact=await (await provider()).request(`/api/lab/evidence?experimentId=${encodeURIComponent(experimentId)}`);
  if(downloadUrl) URL.revokeObjectURL(downloadUrl);
  downloadUrl=URL.createObjectURL(new Blob([JSON.stringify(artifact,null,2)],{type:'application/json'}));
  return downloadUrl;
}
