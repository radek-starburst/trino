/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation;

import io.trino.operator.aggregation.state.LongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDecimalSumAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final DecimalType TYPE = createDecimalType(38, 0);

    private LongLongState decimalState;
    private LongState overflowState;
    AccumulatorStateFactory<LongLongState> longLongStateFactory;
    AccumulatorStateFactory<LongState> longStateFactory;

    @BeforeMethod
    public void setUp()
    {
        longLongStateFactory = StateCompiler.generateStateFactory(LongLongState.class);
        longStateFactory = StateCompiler.generateStateFactory(LongState.class);
        decimalState = longLongStateFactory.createSingleState();
        overflowState = longStateFactory.createSingleState();
    }

    @Test
    public void testOverflow()
    {
        addToState(decimalState, overflowState, TWO.pow(126));

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126)));

        addToState(decimalState, overflowState, TWO.pow(126));

        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(1L << 63, 0));
    }

    @Test
    public void testUnderflow()
    {
        addToState(decimalState, overflowState, TWO.pow(126).negate());

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126).negate()));

        addToState(decimalState, overflowState, TWO.pow(126).negate());

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0x8000000000000000L, 0));
    }

    @Test
    public void testUnderflowAfterOverflow()
    {
        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(125));

        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf((1L << 63) | (1L << 61), 0));

        addToState(decimalState, overflowState, TWO.pow(126).negate());
        addToState(decimalState, overflowState, TWO.pow(126).negate());
        addToState(decimalState, overflowState, TWO.pow(126).negate());

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(125).negate()));
    }

    @Test
    public void testCombineOverflow()
    {
        addToState(decimalState, overflowState, TWO.pow(125));
        addToState(decimalState, overflowState, TWO.pow(126));

        LongLongState otherDecimalState = longLongStateFactory.createSingleState();
        LongState otherOverflowState = longStateFactory.createSingleState();

        addToState(otherDecimalState, otherOverflowState, TWO.pow(125));
        addToState(otherDecimalState, otherOverflowState, TWO.pow(126));

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);
        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0xC000000000000000L, 0));
    }

    @Test
    public void testCombineUnderflow()
    {
        addToState(decimalState, overflowState, TWO.pow(125).negate());
        addToState(decimalState, overflowState, TWO.pow(126).negate());

        LongLongState otherDecimalState = longLongStateFactory.createSingleState();
        LongState otherOverflowState = longStateFactory.createSingleState();

        addToState(otherDecimalState, otherOverflowState, TWO.pow(125).negate());
        addToState(otherDecimalState, otherOverflowState, TWO.pow(126).negate());

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);
        assertEquals(overflowState.getValue(), -1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0x4000000000000000L, 0));
    }

    @Test
    public void testOverflowOnOutput()
    {
        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(126));

        assertEquals(overflowState.getValue(), 1);
        assertThatThrownBy(() -> DecimalSumAggregation.outputLongDecimal(decimalState, overflowState, new VariableWidthBlockBuilder(null, 10, 100)))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    private static void addToState(LongLongState decimalState, LongState overflowState, BigInteger value)
    {
        BlockBuilder blockBuilder = TYPE.createFixedSizeBlockBuilder(1);
        TYPE.writeObject(blockBuilder, Int128.valueOf(value));
        if (TYPE.isShort()) {
            DecimalSumAggregation.inputShortDecimal(decimalState, overflowState, blockBuilder.build().getLong(0, 0));
        }
        else {
            DecimalSumAggregation.inputLongDecimal(decimalState, overflowState, blockBuilder.build(), 0);
        }
    }

    private Int128 getDecimal(LongLongState state)
    {
        return Int128.valueOf(state.getFirst(), state.getSecond());
    }
}
