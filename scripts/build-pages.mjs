import {readFile,writeFile,mkdir,copyFile,readdir,rm} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const root=fileURLToPath(new URL('../',import.meta.url));
const out=path.join(root,'dist');
// Only this fixed generated directory is cleaned, never an environment-provided path.
if(path.dirname(out)!==path.resolve(root)||path.basename(out)!=='dist')throw new Error('Unsafe build destination');
const apiBase=(process.env.MARKETPULSE_API_BASE||'').replace(/\/$/,'');
if(apiBase) {
  const url=new URL(apiBase);
  if(url.protocol!=='https:'||url.username||url.password||url.search||url.hash||url.pathname!=='/')throw new Error('MARKETPULSE_API_BASE must be a public HTTPS origin without credentials, query or path');
}
const publicFiles=['index.html','app.js','style.css','favicon.svg','wind-tunnel.js','wind-tunnel.css','runtime.js','browser-provider.js'];
const sharedFiles=['analytics.mjs','wind-tunnel.mjs'];
for(const file of [...publicFiles.map(name=>path.join('public',name)),...sharedFiles])await readFile(path.join(root,file));
await mkdir(out,{recursive:true});
for(const entry of await readdir(out))await rm(path.join(out,entry),{recursive:true,force:true});
await mkdir(path.join(out,'shared'),{recursive:true});
for(const file of publicFiles)await copyFile(path.join(root,'public',file),path.join(out,file));
for(const file of sharedFiles) {
  const content=await readFile(path.join(root,file),'utf8');
  if(/(?:from\s*|import\s*\()['"]node:/.test(content))throw new Error(`Node dependency in browser shared module: ${file}`);
  await writeFile(path.join(out,'shared',file),content);
}
await writeFile(path.join(out,'runtime-config.js'),`export default Object.freeze(${JSON.stringify({mode:apiBase?'api':'public',apiBase})});\n`);
await writeFile(path.join(out,'.nojekyll'),'');
console.log(`Pages build: ${out}\nData mode: ${apiBase?'explicit API bridge '+apiBase:'Coinbase public REST in the browser'}\nPublished only allowlisted public assets and two pure shared modules.`);
