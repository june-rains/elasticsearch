/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.component;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public class LifecycleTests extends ESTestCase {
    @Test
    public void stateShouldBeInitializedWhenCreate() {
        final var lifecycle = new Lifecycle();
        assertState(lifecycle, Lifecycle.State.INITIALIZED);
    }

    @Test
    public void canMoveToStartedWhenInitialized() {
        final var lifecycle = new Lifecycle();
        assertTrue(lifecycle.moveToStarted());
        assertState(lifecycle, Lifecycle.State.STARTED);
    }

    @Test
    public void moveToStartedReturnFalseWhenStarted() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToStarted();
        assertState(lifecycle, Lifecycle.State.STARTED);
        assertFalse(lifecycle.moveToStarted());
    }

    @Test
    public void moveToStartedReturnFalseWhenStopped() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToStarted();
        lifecycle.moveToStopped();
        assertState(lifecycle, Lifecycle.State.STOPPED);
        AssertionError thrown = assertThrows(AssertionError.class, lifecycle::canMoveToStarted);
        assertEquals("STOPPED -> STARTED", thrown.getMessage());
    }

    @Test
    public void moveToStartedThrowsAssertionErrorExceptionWhenClosed() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToStarted();
        lifecycle.moveToStopped();
        lifecycle.moveToClosed();
        assertState(lifecycle, Lifecycle.State.CLOSED);
        AssertionError thrown = assertThrows(AssertionError.class, lifecycle::canMoveToStarted);
        assertEquals("CLOSED -> STARTED", thrown.getMessage());
    }

    @Test
    public void canMoveToStoppedWhenStarted() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToStarted();
        assertState(lifecycle, Lifecycle.State.STARTED);
        assertTrue(lifecycle.moveToStopped());
        assertState(lifecycle, Lifecycle.State.STOPPED);
    }

    @Test
    public void moveToStoppedThrowAssertionErrorExceptionWhenInitialized() {
        final var lifecycle = new Lifecycle();
        assertState(lifecycle, Lifecycle.State.INITIALIZED);
        AssertionError thrown = assertThrows(AssertionError.class, lifecycle::canMoveToStopped);
        assertEquals("INITIALIZED -> STOPPED", thrown.getMessage());
    }

    @Test
    public void moveToStoppedReturnFalseWhenStopped() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToStarted();
        lifecycle.moveToStopped();
        assertState(lifecycle, Lifecycle.State.STOPPED);
        assertFalse(lifecycle.moveToStopped());
    }

    @Test
    public void moveToStoppedThrowsAssertionErrorExceptionWhenClosed() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToClosed();
        assertState(lifecycle, Lifecycle.State.CLOSED);
        AssertionError thrown = assertThrows(AssertionError.class, lifecycle::canMoveToStopped);
        assertEquals("CLOSED -> STOPPED", thrown.getMessage());
    }

    @Test
    public void canMoveToClosedWhenInitialized() {
        final var lifecycle = new Lifecycle();
        assertState(lifecycle, Lifecycle.State.INITIALIZED);
        assertTrue(lifecycle.moveToClosed());
        assertState(lifecycle, Lifecycle.State.CLOSED);
    }

    @Test
    public void canMoveToClosedWhenStopped() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToStarted();
        lifecycle.moveToStopped();
        assertState(lifecycle, Lifecycle.State.STOPPED);
        assertTrue(lifecycle.moveToClosed());
        assertState(lifecycle, Lifecycle.State.CLOSED);
    }

    @Test
    public void moveToClosedThrowsAssertionErrorExceptionWhenStarted() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToStarted();
        assertState(lifecycle, Lifecycle.State.STARTED);
        AssertionError thrown = assertThrows(AssertionError.class, lifecycle::canMoveToClosed);
        assertEquals("STARTED -> CLOSED", thrown.getMessage());
    }

    @Test
    public void moveToClosedReturnFalseWhenClosed() {
        final var lifecycle = new Lifecycle();
        lifecycle.moveToClosed();
        assertState(lifecycle, Lifecycle.State.CLOSED);
        assertFalse(lifecycle.moveToClosed());
    }




    public void testTransitions() {
        doTransitionTest(false);
        doTransitionTest(true);
    }

    private void doTransitionTest(boolean startBeforeClosing) {
        final var lifecycle = new Lifecycle();

        assertState(lifecycle, Lifecycle.State.INITIALIZED);
        assertTrue(lifecycle.canMoveToStarted());
        assertTrue(lifecycle.canMoveToClosed());

        if (startBeforeClosing) {
            assertTrue(lifecycle.moveToStarted());
            assertState(lifecycle, Lifecycle.State.STARTED);
            assertFalse(lifecycle.canMoveToStarted());
            assertTrue(lifecycle.canMoveToStopped());

            assertTrue(lifecycle.moveToStopped());
            assertState(lifecycle, Lifecycle.State.STOPPED);
            assertFalse(lifecycle.canMoveToStopped());
            assertTrue(lifecycle.canMoveToClosed());
        }

        assertTrue(lifecycle.moveToClosed());
        assertState(lifecycle, Lifecycle.State.CLOSED);
        assertFalse(lifecycle.canMoveToClosed());
    }

    private static void assertState(Lifecycle lifecycle, Lifecycle.State expectedState) {
        assertEquals(expectedState, lifecycle.state());
        assertEquals(expectedState == Lifecycle.State.INITIALIZED, lifecycle.initialized());
        assertEquals(expectedState == Lifecycle.State.STARTED, lifecycle.started());
        assertEquals(expectedState == Lifecycle.State.STOPPED, lifecycle.stopped());
        assertEquals(expectedState == Lifecycle.State.CLOSED, lifecycle.closed());
        assertEquals(expectedState == Lifecycle.State.STOPPED || expectedState == Lifecycle.State.CLOSED, lifecycle.stoppedOrClosed());
    }

    public void testThreadSafety() {
        final var lifecycle = new Lifecycle();

        try (var testHarness = new ThreadSafetyTestHarness(between(1, 10))) {
            assertState(lifecycle, Lifecycle.State.INITIALIZED);
            testHarness.testTransition(lifecycle::moveToStarted);
            assertState(lifecycle, Lifecycle.State.STARTED);
            testHarness.testTransition(lifecycle::moveToStopped);
            assertState(lifecycle, Lifecycle.State.STOPPED);
            testHarness.testTransition(lifecycle::moveToClosed);
            assertState(lifecycle, Lifecycle.State.CLOSED);
        }
    }

    private static class ThreadSafetyTestHarness implements Releasable {
        final int threads;
        final CyclicBarrier barrier;
        final ExecutorService executor;

        ThreadSafetyTestHarness(int threads) {
            this.threads = threads;
            this.barrier = new CyclicBarrier(threads);
            this.executor = EsExecutors.newScaling(
                "test",
                threads,
                threads,
                10,
                TimeUnit.SECONDS,
                true,
                EsExecutors.daemonThreadFactory("test"),
                new ThreadContext(Settings.EMPTY)
            );
        }

        void testTransition(BooleanSupplier doTransition) {
            final var transitioned = new AtomicBoolean();
            PlainActionFuture.<Void, RuntimeException>get(fut -> {
                try (var listeners = new RefCountingListener(fut)) {
                    for (int i = 0; i < threads; i++) {
                        executor.execute(ActionRunnable.run(listeners.acquire(), () -> {
                            safeAwait(barrier);
                            if (doTransition.getAsBoolean()) {
                                assertTrue(transitioned.compareAndSet(false, true));
                            }
                        }));
                    }
                }
            });
            assertTrue(transitioned.get());
        }

        @Override
        public void close() {
            terminate(executor);
        }
    }
}
