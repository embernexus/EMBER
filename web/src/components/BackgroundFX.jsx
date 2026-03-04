import { useEffect, useRef } from "react";

export function FireBg() {
  const ref = useRef(null);
  useEffect(() => {
    const canvas = ref.current; if (!canvas) return;
    const ctx = canvas.getContext("2d");
    let raf;
    const resize = () => { canvas.width = window.innerWidth; canvas.height = window.innerHeight; };
    resize(); window.addEventListener("resize", resize);
    const COLS = [[255,255,180],[255,155,20],[255,75,0],[200,25,0],[100,8,0]];
    let pts = Array.from({length:32}, () => {
      const p = { x:Math.random()*window.innerWidth, y:window.innerHeight+10, vx:(Math.random()-.5)*.5, vy:-(Math.random()*1.6+.5), life:Math.random(), sz:Math.random()*90+25, w:Math.random()*Math.PI*2 };
      p.y = window.innerHeight - Math.random()*window.innerHeight*.35;
      return p;
    });
    const spawn = () => ({ x:Math.random()*canvas.width, y:canvas.height+10, vx:(Math.random()-.5)*.5, vy:-(Math.random()*1.6+.5), life:1, sz:Math.random()*90+25, w:Math.random()*Math.PI*2 });
    const draw = () => {
      ctx.clearRect(0,0,canvas.width,canvas.height);
      const t = Date.now()*.001;
      pts.forEach((p,i) => {
        p.x += p.vx + Math.sin(t*1.1+p.w+i*.25)*.45;
        p.y += p.vy; p.life -= .0035; p.sz *= .998;
        if (p.life <= 0 || p.y < -p.sz) { pts[i] = spawn(); return; }
        const ci = Math.min(Math.floor((1-p.life)*COLS.length), COLS.length-1);
        const [r,g,b] = COLS[ci];
        const g2 = ctx.createRadialGradient(p.x,p.y,0,p.x,p.y,p.sz);
        g2.addColorStop(0,`rgba(${r},${g},${b},${p.life*.16})`);
        g2.addColorStop(.5,`rgba(${r},${Math.floor(g*.5)},0,${p.life*.08})`);
        g2.addColorStop(1,"rgba(0,0,0,0)");
        ctx.beginPath(); ctx.arc(p.x,p.y,p.sz,0,Math.PI*2); ctx.fillStyle=g2; ctx.fill();
      });
      raf = requestAnimationFrame(draw);
    };
    draw();
    return () => { cancelAnimationFrame(raf); window.removeEventListener("resize",resize); };
  },[]);
  return <canvas ref={ref} style={{position:"fixed",inset:0,width:"100%",height:"100%",pointerEvents:"none",zIndex:0,opacity:.85}} />;
}

export function Embers() {
  const items = useRef(Array.from({length:16},(_,i)=>({ id:i, left:`${5+Math.random()*90}%`, delay:`${Math.random()*14}s`, dur:`${7+Math.random()*9}s`, sz:Math.random()>.5?3:2, col:Math.random()>.5?"#ff8c42":"#ff4500" }))).current;
  return (
    <div style={{position:"fixed",inset:0,pointerEvents:"none",zIndex:1,overflow:"hidden"}}>
      {items.map(e=>(
        <div key={e.id} style={{position:"absolute",bottom:0,left:e.left,width:e.sz,height:e.sz,borderRadius:"50%",background:e.col,boxShadow:`0 0 ${e.sz*3}px ${e.col}`,animation:`floatUp ${e.dur} ${e.delay} infinite ease-out`,opacity:0}} />
      ))}
    </div>
  );
}
