import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { createOfferService, makeShopify, loadConfig, endOfJstDay } from './offers.mjs';
import { createLpRouter } from './router.mjs';
const campaign='libase-swipe-2026';
const config={enabled:true,campaign,origin:'https://libase.shop',secret:'a'.repeat(64),dailyLimit:1000};
const body={campaign,visitorId:'a'.repeat(48)};
function fixture(options={}) {
 let now=Date.parse('2026-09-27T22:00:00+09:00');
 const rows=new Map(),remote=new Map();let creates=0,failed=false;
 const store={
  async reserve(record,limit){
   if(rows.has(record.visitor_key))return rows.get(record.visitor_key);
   if([...rows.values()].filter(r=>r.created_at>=endOfJstDay(now)-86400000).length>=limit)throw Object.assign(new Error('limit'),{status:429});
   rows.set(record.visitor_key,{...record,state:'pending',checked_at:0});return rows.get(record.visitor_key);
  },
  async withVisit(key,fn){return fn(rows.get(key),async(state,checked_at)=>Object.assign(rows.get(key),{state,checked_at}));},
  async markUsed(codes){for(const r of rows.values())if(codes.includes(r.code))r.state='used';}
 };
 const shopify={async find(code){return remote.get(code)||null;},async create(record){
  creates++;const r={status:'ACTIVE',endsAt:new Date(record.ends_at).toISOString(),usageLimit:1,asyncUsageCount:0,customerGets:{items:{__typename:'AllDiscountItems'},value:{percentage:0.1}}};remote.set(record.code,r);
  if(options.failOnce&&!failed){failed=true;throw Error('response lost');}
 }};
 const settings={...config,...options.config};
 const service=()=>createOfferService(settings,{store,shopify,clock:()=>now});
 return {service,rows,remote,get creates(){return creates;},setNow:value=>now=Date.parse(value),advance:ms=>now+=ms};
}
test('same-day deadline, simultaneous claims, persisted record and next-day revisit',async()=>{
 const f=fixture();const s=f.service();const offers=await Promise.all(Array.from({length:8},()=>s.offer(body)));
 assert.equal(f.creates,1);assert.equal(new Set(offers.map(o=>o.code)).size,1);
 const a=offers[0];assert.equal(a.scope,'all');assert.equal(a.expiresAt,'2026-09-27T15:00:00.000Z');
 assert.equal((await f.service().status({campaign,visitorToken:a.visitorToken})).code,a.code);
 f.setNow('2026-09-28T00:00:00+09:00');assert.equal((await f.service().offer(body)).status,'expired');assert.equal(f.creates,1);
});
test('status cannot grant eligibility or mint coupons; forged tokens are rejected',async()=>{
 const f=fixture();const s=f.service();await assert.rejects(s.status({campaign,visitorToken:'a'.repeat(64)+'.'+'b'.repeat(64)}),e=>e.status===401);
 await assert.rejects(s.offer({campaign,visitorId:'short'}),e=>e.status===400);
 await assert.rejects(s.offer({...body,campaign:'different'}),e=>e.status===400);
 const a=await s.offer(body);const row=[...f.rows.values()][0];row.state='pending';f.remote.clear();
 assert.equal((await s.status({campaign,visitorToken:a.visitorToken})).status,'pending');assert.equal(f.creates,1);
});
test('uncertain creation recovers deterministic code without extending expiry',async()=>{
 const f=fixture({failOnce:true});await assert.rejects(f.service().offer(body));const row=[...f.rows.values()][0];const end=row.ends_at;
 f.advance(120000);const a=await f.service().offer(body);assert.equal(a.status,'active');assert.equal(Date.parse(a.expiresAt),end);assert.equal(f.creates,1);
});
test('used, removed, modified coupons and disabled campaign fail closed',async()=>{
 const f=fixture();const s=f.service();const a=await s.offer(body);
 await s.markUsed([{code:a.code}]);assert.equal((await s.status({campaign,visitorToken:a.visitorToken})).status,'used');
 const f2=fixture();const b=await f2.service().offer(body);f2.remote.clear();f2.advance(61000);
 assert.equal((await f2.service().status({campaign,visitorToken:b.visitorToken})).status,'expired');assert.equal(f2.creates,1);
 const f3=fixture();const c=await f3.service().offer(body);f3.remote.get(c.code).customerGets.value.percentage=0.2;f3.advance(61000);
 await assert.rejects(f3.service().status({campaign,visitorToken:c.visitorToken}),e=>e.status===503);
 assert.equal((await fixture({config:{enabled:false}}).service().offer(body)).status,'disabled');
});
test('daily cap applies only to new visitors',async()=>{
 const f=fixture({config:{dailyLimit:1}});const s=f.service();await s.offer(body);await s.offer(body);
 await assert.rejects(s.offer({...body,visitorId:'b'.repeat(48)}),e=>e.status===429);
});
test('Shopify mutation grants all one-time products, 10%, one use, fixed expiry, no stacking',async()=>{
 let captured;
 const shop=makeShopify({shop:'example.myshopify.com',getAccessToken:async()=>'test-token',fetcher:async(url,init)=>{
  captured={url,init,body:JSON.parse(init.body)};return {ok:true,json:async()=>({data:{discountCodeBasicCreate:{codeDiscountNode:{id:'id'},userErrors:[]}}})};
 }});
 await shop.create({campaign,code:'LP'+'A'.repeat(24),created_at:1,ends_at:2});
 assert.deepEqual(captured.body.variables.input.customerGets,{value:{percentage:0.1},items:{all:true},appliesOnOneTimePurchase:true,appliesOnSubscription:false});
 assert.equal(captured.body.variables.input.endsAt,'1970-01-01T00:00:00.002Z');assert.equal(captured.body.variables.input.usageLimit,1);
 assert.deepEqual(captured.body.variables.input.combinesWith,{orderDiscounts:false,productDiscounts:false,shippingDiscounts:false});
 assert.equal(captured.init.headers['X-Shopify-Access-Token'],'test-token');
});
test('stores without subscriptions retry only the unsupported purchase-type fields',async()=>{
 const inputs=[];
 const shop=makeShopify({shop:'example.myshopify.com',getAccessToken:async()=>'test-token',fetcher:async(url,init)=>{
  inputs.push(JSON.parse(init.body).variables.input);
  const result=inputs.length===1 ? {codeDiscountNode:null,userErrors:['appliesOnSubscription','appliesOnOneTimePurchase'].map(field=>({field:['basicCodeDiscount','customerGets',field],code:'INVALID',message:'field is not permitted without the shop using subscriptions.'}))} : {codeDiscountNode:{id:'id'},userErrors:[]};
  return {ok:true,json:async()=>({data:{discountCodeBasicCreate:result}})};
 }});
 assert.equal(await shop.create({code:'LP'+'A'.repeat(24),created_at:1,ends_at:2}),'id');
 assert.equal(inputs.length,2);
 assert.deepEqual(inputs[1],{...inputs[0],customerGets:{value:{percentage:0.1},items:{all:true}}});
});
test('isolated HTTP routes enforce origin, JSON size and status/offer separation',async()=>{
 const f=fixture();const app=express();app.use('/lp',createLpRouter({env:{LP_ENABLED:'true',LP_VISITOR_SECRET:config.secret},service:f.service()}).router);
 app.get('/legacy',(_req,res)=>res.send('unchanged'));
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.on('listening',r));const url=`http://127.0.0.1:${server.address().port}`;
 try{
  assert.equal((await fetch(url+'/legacy')).status,200);
  assert.equal((await fetch(url+'/lp/health')).status,200);
  const request=(path,payload,origin=config.origin)=>fetch(url+path,{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify(payload)});
  assert.equal((await request('/lp/offer',body,'https://other.test')).status,403);
  assert.equal((await request('/lp/offer',{...body,extra:'x'.repeat(3000)})).status,413);
  const response=await request('/lp/offer',body);assert.equal(response.status,200);assert.equal(response.headers.get('access-control-allow-origin'),config.origin);
  const offer=await response.json();assert.equal((await (await request('/lp/status',{campaign,visitorToken:offer.visitorToken})).json()).status,'active');
 }finally{await new Promise(r=>server.close(r));}
});
test('disabled feature does not require new secrets; enabled feature does',()=>{
 assert.equal(loadConfig({}).enabled,false);assert.throws(()=>loadConfig({LP_ENABLED:'true'}));
});
