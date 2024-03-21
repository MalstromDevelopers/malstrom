import { Circle, Txt, Polygon, makeScene2D, CubicBezier, Ray } from '@motion-canvas/2d';
import { all, createRef, createSignal, waitFor } from '@motion-canvas/core';
import {CodeBlock, edit, range} from '@motion-canvas/2d/lib/components/CodeBlock';

export default makeScene2D(function* (view) {
  // https://www.realtimecolors.com/dashboard?colors=f3fcfc-072022-76ecf2-0e5498-eb281e&fonts=Noto%20Sans-Noto%20Sans
  const light = {
    background: '#ddf6f8',
    text: '#030c0c',
    primary: '#0d8187',
    secondary: '#9cb3d4',
    accent: '#e11e14',
  }

  const dark = {
    background: '#072022',
    text: '#f3fcfc',
    primary: '#47b2b855',
    secondary: '#0e549855',
    accent: '#eb281edd',
  }


  const theme = dark
  const nodeSize = 200
  view.fill(theme.background)


  const streamA = createRef<Circle>();
  const streamB = createRef<Circle>();
  const join = createRef<Polygon>();

  // source nodes
  view.add(
    <Circle
      ref={streamA}
      x={-600}
      y={-200}
      opacity={0.5}
      width={nodeSize}
      height={nodeSize}
      fill={theme.primary}
    ><Txt fill={theme.text}>Stream A</Txt></Circle>,
  );
  view.add(
    <Circle
      ref={streamB}
      x={-600}
      y={200}
      opacity={0.5}
      width={nodeSize}
      height={nodeSize}
      fill={theme.secondary}
    ><Txt fill={theme.text}>Stream B</Txt></Circle>,
  );

  // join
  view.add(
    <Circle
      ref={join}
      x={100}
      y={0}
      opacity={0.5}
      width={nodeSize}
      height={nodeSize}
      fill={theme.accent }
    ><Txt fill={theme.text}>Join</Txt></Circle>,
  );

  // edges into join
  const edgeA = createRef<CubicBezier>();
  const edgeB = createRef<CubicBezier>();

  view.add(
    <CubicBezier
      ref={edgeA}
      lineWidth={6}
      stroke={'lightseagreen'}
      p0={[-500, -200]}
      p1={[-330, -200]}
      p2={[-150, -10]}
      p3={[0, -10]}
    />,
  );
  view.add(
    <CubicBezier
      ref={edgeB}
      lineWidth={6}
      stroke={'lightseagreen'}
      p0={[-500, 200]}
      p1={[-330, 200]}
      p2={[-150, 10]}
      p3={[0, 10]}
    />,
  );

  const edgeOut = createRef<Ray>();
  view.add(
    <Ray
    ref={edgeOut}
    lineWidth={6}
    stroke={'lightseagreen'}
    fromX={join().x() + join().width() / 2}
    toX={1000}
    />
  )

  // Epoch comparison code
  const codeRef = createRef<CodeBlock>();
  view.add(
    <CodeBlock language="rust"
    ref={codeRef}
    x={400}
    y={-250}
    fontSize={40}
    selection={range(2, 0, 2, 100)}
    code={`
    impl Timestamp for i32 {
      fn merge(&self, other: &i32) -> i32 {
        min(self, other)
      }
    }
    `} />,
  )

  // epochs
  const epochA1 = createRef<Circle>();
  const epochA1Progress = createSignal(0);
  view.add(
    <Circle
      ref={epochA1}
      position={() => edgeA().getPointAtPercentage(epochA1Progress()).position}
      width={70}
      height={70}
      fill={theme.primary}
    ><Txt fill={theme.text} fontSize={30}>7</Txt></Circle>,
  );

  const epochB1 = createRef<Circle>();
  const epochB1Progress = createSignal(0);
  view.add(
    <Circle
      ref={epochB1}
      position={() => edgeB().getPointAtPercentage(epochB1Progress()).position}
      width={70}
      height={70}
      fill={theme.secondary}
      opacity={0}
    ><Txt fill={theme.text} fontSize={30}>42</Txt></Circle>,
  );

  const epochOut1 = createRef<Circle>();
  const epochOut1Progress = createSignal(0);
  view.add(
    <Circle
      ref={epochOut1}
      position={() => edgeOut().getPointAtPercentage(epochOut1Progress()).position}
      width={70}
      height={70}
      fill={theme.primary}
      opacity={0}
    ><Txt fill={theme.text} fontSize={30}>7</Txt></Circle>,
  );


  // yield* epochA1Progress(0., 1.5)
  yield* epochA1Progress(1, 1)//,
  yield* all(epochA1().position.y(-50,  0.2), epochB1().opacity(1, 0.2))
  yield* epochB1Progress(1, 1)
  yield* epochB1().position.y(50,  0.2)

  // make code visible to explain the merge
  yield* codeRef().edit(1.5)`
  impl Timestamp for i32 {
    fn merge(&self, other: &i32) -> i32 {
      min(${edit('self', '7')}, ${edit('other', '42')})
    }
  }
  `;
  yield waitFor(0.5)
  yield* codeRef().edit(1)`
  impl Timestamp for i32 {
    fn merge(&self, other: &i32) -> i32 {
      ${edit('min(7, 42)', ' = 7')}
    }
  }
  `;


  yield* epochOut1().opacity(1, 0.2)
  yield* epochOut1Progress(1, 1)

  // reset codeblock
  yield* codeRef().edit(1)`
  impl Timestamp for i32 {
    fn merge(&self, other: &i32) -> i32 {
      ${edit(' = 7', 'min(self, other)')}
    }
  }
  `;

  const epochA2 = createRef<Circle>();
  const epochA2Progress = createSignal(0);
  view.add(
    <Circle
      ref={epochA2}
      position={() => edgeA().getPointAtPercentage(epochA2Progress()).position}
      width={70}
      height={70}
      fill={theme.primary}
    ><Txt fill={theme.text} fontSize={30}>100</Txt></Circle>,
  );
  yield* epochA2Progress(1, 1)
  yield* all(epochA2().position.y(-50,  0.2), epochA1().opacity(0, 0.2))

  yield* codeRef().selection(range(2, 0, 2, 100), 0.2)
  yield* codeRef().edit(1.5)`
  impl Timestamp for i32 {
    fn merge(&self, other: &i32) -> i32 {
      min(${edit('self', '100')}, ${edit('other', '42')})
    }
  }
  `;
  yield waitFor(0.5)
  yield* codeRef().edit(1)`
  impl Timestamp for i32 {
    fn merge(&self, other: &i32) -> i32 {
      ${edit('min(100, 42)', ' = 42')}
    }
  }
  `;
  const epochOut2 = createRef<Circle>();
  const epochOut2Progress = createSignal(0);
  view.add(
    <Circle
      ref={epochOut2}
      position={() => edgeOut().getPointAtPercentage(epochOut2Progress()).position}
      width={70}
      height={70}
      fill={theme.secondary}
      opacity={0}
    ><Txt fill={theme.text} fontSize={30}>42</Txt></Circle>,
  );
  yield* epochOut2().opacity(1, 0.2)
  yield* epochOut2Progress(1, 1)

  // reset codeblock
  yield* codeRef().edit(1)`
  impl Timestamp for i32 {
    fn merge(&self, other: &i32) -> i32 {
      ${edit(' = 42', 'min(self, other)')}
    }
  }
  `;

  yield* waitFor(2)
 
});
