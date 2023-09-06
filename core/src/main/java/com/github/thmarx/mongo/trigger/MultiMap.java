/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.trigger;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MultiMap<K, V> {

	private Map<K, Collection<V>> map = new HashMap<>();

	/**
	 * Addieren Sie den angegebenen Wert mit dem angegebenen Schlüssel in dieser Multimap.
	 */
	public void put(K key, V value) {
		if (map.get(key) == null) {
			map.put(key, new ArrayList<>());
		}

		map.get(key).add(value);
	}

	/**
	 * Verknüpfen Sie den angegebenen Schlüssel mit dem angegebenen Wert, wenn nicht bereits mit einem Wert verknüpft
	 */
	public void putIfAbsent(K key, V value) {
		if (map.get(key) == null) {
			map.put(key, new ArrayList<>());
		}

		// Wenn der Wert fehlt, füge ihn ein
		if (!map.get(key).contains(value)) {
			map.get(key).add(value);
		}
	}

	/**
	 * Gibt die Sammlung von Werten zurück, denen der angegebene Schlüssel zugeordnet ist, oder null, wenn diese
	 * Multimap keine Zuordnung für den Schlüssel enthält.
	 */
	public Collection<V> get(Object key) {
		if (!map.containsKey(key)) {
			return Collections.EMPTY_LIST;
		}
		return map.get(key);
	}

	/**
	 * Gibt eine Set-Ansicht der in dieser Multimap enthaltenen Schlüssel zurück.
	 */
	public Set<K> keySet() {
		return map.keySet();
	}

	/**
	 * Gibt eine Set-Ansicht der in dieser Multimap enthaltenen Mappings zurück.
	 */
	public Set<Map.Entry<K, Collection<V>>> entrySet() {
		return map.entrySet();
	}

	/**
	 * Gibt eine Sammlungsansicht der Sammlung der in vorhandenen Werte zurück diese Multimap.
	 */
	public Collection<Collection<V>> values() {
		return map.values();
	}

	/**
	 * Gibt „true“ zurück, wenn diese Multimap eine Zuordnung für den angegebenen Schlüssel enthält.
	 */
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	/**
	 * Entfernt die Zuordnung für den angegebenen Schlüssel aus dieser Multimap, falls vorhanden und gibt die Sammlung
	 * früherer Werte zurück, die dem Schlüssel oder zugeordnet sind null, wenn keine Schlüsselzuordnung vorhanden war.
	 */
	public Collection<V> remove(Object key) {
		return map.remove(key);
	}

	/**
	 * Gibt die Gesamtzahl der Schlüsselwertzuordnungen in dieser Multimap zurück.
	 */
	public int size() {
		int size = 0;
		for (Collection<V> value : map.values()) {
			size += value.size();
		}
		return size;
	}

	/**
	 * Gibt „true“ zurück, wenn diese Multimap keine Schlüsselwertzuordnungen enthält.
	 */
	public boolean isEmpty() {
		return map.isEmpty();
	}

	/**
	 * Entfernt alle Mappings von dieser Multimap.
	 */
	public void clear() {
		map.clear();
	}

	/**
	 * Entfernt den Eintrag für den angegebenen Schlüssel nur, wenn dies derzeit der Fall ist wird dem angegebenen Wert
	 * zugeordnet und gibt „true“ zurück, wenn es entfernt wird
	 */
	public boolean remove(K key, V value) {
		if (map.get(key) != null) // Schlüssel existiert
		{
			return map.get(key).remove(value);
		}

		return false;
	}

	/**
	 * Ersetzt den Eintrag für den angegebenen Schlüssel nur, falls vorhanden dem angegebenen Wert zugeordnet und bei
	 * Ersetzung wahr zurückgeben
	 */
	public boolean replace(K key, V oldValue, V newValue) {
		if (map.get(key) != null) {
			if (map.get(key).remove(oldValue)) {
				return map.get(key).add(newValue);
			}
		}
		return false;
	}
}
