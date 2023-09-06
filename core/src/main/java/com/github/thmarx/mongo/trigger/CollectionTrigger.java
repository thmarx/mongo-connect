/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package com.github.thmarx.mongo.trigger;

import java.util.function.BiConsumer;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
@FunctionalInterface
public interface CollectionTrigger {

	void accept(Type type, String database, String colection);

	public enum Type {
		DROPPED;
	}
}
