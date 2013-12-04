package org.greenscape.greendb.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.Entity;

import org.greenscape.greendb.Connection;
import org.greenscape.persistence.DocumentModel;
import org.greenscape.persistence.DocumentModelBase;
import org.greenscape.persistence.PersistedModelBase;
import org.greenscape.persistence.PersistenceProvider;
import org.greenscape.persistence.PersistenceService;
import org.greenscape.persistence.PersistenceType;
import org.greenscape.persistence.util.PersistenceFactoryUtil;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * 
 * @author Sheikh Sajid
 * 
 */
@Component
public class GreenDBPersistence implements PersistenceService {

	private static final String PROVIDER_NAME = "GreenDB";
	private static final PersistenceProvider provider;

	private Connection connection;

	private ODatabaseDocument docbase;

	static {
		provider = PersistenceFactoryUtil.createPersistenceProvider(PROVIDER_NAME, PersistenceType.DOCUMENT);
	}

	@Override
	public PersistenceProvider getProvider() {
		return provider;
	}

	@Override
	public PersistenceType getType() {
		return provider.getType();
	}

	@Reference
	public void setConnection(Connection connection) {
		this.connection = connection;
		this.docbase = this.connection.getDatabaseDocument();
	}

	public void unsetConnection(Connection connection) {
		this.connection = null;
		if (docbase != null) {
			this.docbase.close();
		}
		this.docbase = null;
	}

	@Override
	public <T> void save(T object) {
		ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		persistNonThreaded((DocumentModel) object, true);
	}

	@Override
	public <T> void update(T object) {
		ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		persistNonThreaded((DocumentModel) object, false);
	}

	@Override
	public <T> void saveOrUpdate(T object) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void save(Collection<T> objects) {
		save(objects.toArray());
	}

	@Override
	public <T> void save(T[] objects) {
		if (objects == null || objects.length == 0) {
			return;
		}
		ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		for (Object model : objects) {
			persistNonThreaded((DocumentModel) model, true);
		}
	}

	@Override
	public <T> void remove(T object) {
		ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		removeNonThreaded((DocumentModel) object);
	}

	@Override
	public <T> void remove(Collection<T> objects) {
		remove(objects.toArray());
	}

	@Override
	public <T> void remove(T[] objects) {
		if (objects == null || objects.length == 0) {
			ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		}
		for (Object model : objects) {
			removeNonThreaded((DocumentModel) model);
		}
	}

	@Override
	public Object executeQuery(String query) {
		List<ODocument> list = docbase.query(new OSQLSynchQuery<>(query));
		List<DocumentModelBase> modelList = new ArrayList<>();
		if (list != null && !list.isEmpty()) {
			for (ODocument doc : list) {
				if (doc.getClassName() == null) {
					DocumentModelBase model = new DocumentModelBase();
					copy(model, doc);
					modelList.add(model);
				} else {
					PersistedModelBase model = new PersistedModelBase();
					copy(model, doc);
					modelList.add(model);
				}
			}
		}
		return modelList;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Collection<T> executeQuery(Class<T> clazz, String query) {
		List<T> modelList = new ArrayList<>();
		if (docbase.existsCluster(clazz.getAnnotation(Entity.class).name().toLowerCase())) {
			List<ODocument> list = docbase.query(new OSQLSynchQuery<>(query));
			if (list != null && list.size() > 0) {
				for (ODocument doc : list) {
					DocumentModel model = null;
					try {
						model = (DocumentModel) clazz.newInstance();
						copy(model, doc);
					} catch (InstantiationException | IllegalAccessException e) {
						throw new RuntimeException(e);
					}
					modelList.add((T) model);
				}
			}
		}
		return modelList;
	}

	@Override
	public <T> Collection<T> executeQuery(Class<T> clazz, String query, int maxResult) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T findById(Class<T> clazz, Object id) {
		ODocument doc = docbase.load((ORecordId) id);
		DocumentModel model = null;
		try {
			model = (DocumentModel) clazz.newInstance();
			copy(model, doc);
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return (T) model;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> findByProperty(Class<T> clazz, String propertyName, Object value) {
		if (!clazz.isAnnotationPresent(Entity.class)) {
			throw new RuntimeException("No Entity annotation found on class " + clazz.getCanonicalName());
		}
		String modelName = clazz.getAnnotation(Entity.class).name();
		if (modelName == null) {
			modelName = clazz.getSimpleName();
		}
		if (!modelExists(modelName)) {
			// throw new RuntimeException("No model with name " + modelName +
			// " exists");
		}
		List<ODocument> result = docbase.query(new OSQLSynchQuery<>("select from " + modelName + " where "
				+ propertyName + " = ?"), value);

		List<DocumentModel> list = new ArrayList<>();
		DocumentModel model;
		for (ODocument doc : result) {
			try {
				model = (DocumentModel) clazz.newInstance();
				copy(model, doc);
				list.add(model);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		return (List<T>) list;
	}

	@Override
	public <T> void delete(T documentModel) {
		// TODO: gymnastics because of jackson bug, need to upgrade to 2.x
		ORecordId id = new ORecordId((String) ((DocumentModel) documentModel).getId());
		docbase.delete(id);
	}

	@Override
	public boolean modelExists(String modelName) {
		return docbase.existsCluster(modelName.toLowerCase());
	}

	@Override
	public PersistenceService begin() {
		docbase.begin();
		return this;
	}

	@Override
	public PersistenceService commit() {
		docbase.commit();
		return this;
	}

	@Override
	public PersistenceService rollback() {
		docbase.rollback();
		return this;
	}

	private <T extends DocumentModel> T persistNonThreaded(T object, boolean create) {
		if (object == null) {
			return null;
		}
		if (!object.getClass().isAnnotationPresent(Entity.class)) {
			throw new RuntimeException("No Entity annotation found on class " + object.getClass().getCanonicalName());
		}
		String modelName = object.getClass().getAnnotation(Entity.class).name();
		if (modelName == null) {
			modelName = object.getClass().getSimpleName();
		}
		ODocument doc = null;
		if (create) {
			doc = new ODocument(modelName);
		} else {
			// TODO: gymnastics because of jackson bug, need to upgrade to 2.x
			ORecordId id = new ORecordId((String) object.getId());
			doc = docbase.load(id);
		}
		doc.fields(object.getProperties());
		doc.save(create);
		object.setId(doc.getIdentity());
		return object;
	}

	private <T extends DocumentModel> void removeNonThreaded(T object) {
		docbase.delete((ORecordId) object.getId());
	}

	private void copy(DocumentModel model, ODocument doc) {
		for (String field : doc.fieldNames()) {
			model.setProperty(field, doc.field(field));
		}
		// TODO: converting to String because of jackson bug
		model.setId(doc.getIdentity().toString());
	}
}
