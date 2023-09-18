package com.github.thmarx.mongo.connect;

/*-
 * #%L
 * mongo-trigger-index
 * %%
 * Copyright (C) 2023 Marx-Software
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

/**
 *
 * @author t.marx
 */
public abstract class PausableThread implements Runnable {

	private boolean running = true;
	
	private boolean paused = false;
	
	private final Object pauseLock = new Object();
	
	public PausableThread (final boolean paused) {
		if (paused) {
			pause();
		}
	}
	
	public void stop () {
		running = false;
	}
	
	@Override
	public void run() {
		while (running) {
			synchronized (pauseLock) {
                if (!running) { // may have changed while waiting to
                    // synchronize on pauseLock
                    break;
                }
                if (paused) {
                    try {
                        pauseLock.wait(); // will cause this Thread to block until 
                        // another thread calls pauseLock.notifyAll()
                        // Note that calling wait() will 
                        // relinquish the synchronized lock that this 
                        // thread holds on pauseLock so another thread
                        // can acquire the lock to call notifyAll()
                        // (link with explanation below this code)
                    } catch (InterruptedException ex) {
                        break;
                    }
                    if (!running) { // running might have changed since we paused
                        break;
                    }
                }
            }
			
			update();
		}
	}
	
	public void pause () {
		paused = true;
	}
	
	public void unpause () {
		synchronized (pauseLock) {
            paused = false;
            pauseLock.notifyAll(); // Unblocks thread
        }
	}
	
	abstract public void update ();
}
