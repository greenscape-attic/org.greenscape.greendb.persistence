package org.greenscape.greendb.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.management.Query;

import org.greenscape.core.ModelResource;
import org.greenscape.core.Resource;
import org.greenscape.core.ResourceRegistry;
import org.greenscape.core.ResourceType;
import org.greenscape.greendb.Connection;
import org.greenscape.persistence.DocumentModel;
import org.greenscape.persistence.DocumentModelBase;
import org.greenscape.persistence.PersistedModelBase;
import org.greenscape.persistence.PersistenceProvider;
import org.greenscape.persistence.PersistenceService;
import org.greenscape.persistence.PersistenceType;
import org.greenscape.persistence.TypedQuery;
import org.greenscape.persistence.annotations.Model;
import org.greenscape.persistence.criteria.CriteriaBuilder;
import org.greenscape.persistence.criteria.CriteriaDelete;
import org.greenscape.persistence.criteria.CriteriaQuery;
import org.greenscape.persistence.criteria.CriteriaUpdate;
import org.greenscape.persistence.util.PersistenceFactoryUtil;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.log.LogService;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 *
 * @author Sheikh Sajid
 *
 */
@Component(property = { "dbName=" + GreenDBPersistence.PROVIDER_NAME })
public class GreenDBPersistence implements PersistenceService {

	static final String PROVIDER_NAME = "GreenDB";
	private static final PersistenceProvider provider;
	private static final String GREENDB_ID_FIELD = "id";

	private Connection connection;
	private ODatabaseDocument docbase;
	private ResourceRegistry resourceRegistry;

	private BundleContext context;
	private LogService logService;

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

	@Activate
	void activate(ComponentContext ctx, Map<String, Object> config) {
		context = ctx.getBundleContext();
	}

	@Reference(policy = ReferencePolicy.DYNAMIC)
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
	public <T> void save(String modelName, T object) {
		ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		persistNonThreaded(modelName, (DocumentModel) object, true);
	}

	@Override
	public <T> void save(T object) {
		ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		persistNonThreaded((DocumentModel) object, true);
	}

	@Override
	public <T> void update(String modelName, T object) {
		ODatabaseRecordThreadLocal.INSTANCE.set(docbase);
		persistNonThreaded(modelName, (DocumentModel) object, false);
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
		logService.log(LogService.LOG_INFO, "Executing query: " + query);
		List<ODocument> list = docbase.query(new OSQLSynchQuery<>(query));
		List<DocumentModelBase> modelList = new ArrayList<>();
		try {
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
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return modelList;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Collection<T> executeQuery(Class<T> clazz, String query) {
		List<T> modelList = new ArrayList<>();
		if (docbase.existsCluster(clazz.getAnnotation(Model.class).name().toLowerCase())) {
			logService.log(LogService.LOG_INFO, "Executing query: " + query);
			List<ODocument> list = docbase.query(new OSQLSynchQuery<>(query));
			if (list != null && list.size() > 0) {
				for (ODocument doc : list) {
					DocumentModel model = null;
					try {
						model = (DocumentModel) clazz.newInstance();
						copy(model, doc);
					} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
						throw new RuntimeException(e);
					}
					modelList.add((T) model);
				}
			}
		}
		return modelList;
	}

	@SuppressWarnings("unchecked")
	public <T extends DocumentModel> List<T> executeQuery(String modelName, String query, Map<String, Object> params) {
		List<T> modelList = new ArrayList<>();
		if (docbase.existsCluster(modelName.toLowerCase())) {
			logService.log(LogService.LOG_INFO, "Executing query: " + query);
			OSQLSynchQuery<ODocument> oquery = new OSQLSynchQuery<ODocument>(query);
			List<ODocument> list = docbase.command(oquery).execute(params);
			if (list != null && list.size() > 0) {
				try {
					ModelResource modelResource = (ModelResource) resourceRegistry.getResource(modelName);
					Class<?> clazz = null;
					if (modelResource.getModelClass() != null) {
						clazz = context.getBundle(modelResource.getBundleId()).loadClass(modelResource.getModelClass());
					}
					T model = null;
					for (ODocument doc : list) {
						if (clazz == null) {
							model = (T) new PersistedModelBase();
						} else {
							model = (T) clazz.newInstance();
						}

						copy(model, doc);

						modelList.add(model);
					}
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return modelList;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> executeQuery(Class<T> clazz, String query, Map<String, Object> params) {
		List<T> modelList = new ArrayList<>();
		if (docbase.existsCluster(clazz.getAnnotation(Model.class).name().toLowerCase())) {
			logService.log(LogService.LOG_INFO, "Executing query: " + query);
			OSQLSynchQuery<ODocument> oquery = new OSQLSynchQuery<ODocument>(query);
			List<ODocument> list = docbase.command(oquery).execute(params);
			if (list != null && list.size() > 0) {
				for (ODocument doc : list) {
					DocumentModel model = null;
					try {
						model = (DocumentModel) clazz.newInstance();
						copy(model, doc);
					} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
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

	public Object executeUpdate(String query) {
		return docbase.command(new OCommandSQL(query)).execute();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends DocumentModel> List<T> find(String modelName) {
		ORecordIteratorClass<ODocument> itr = docbase.browseClass(modelName.toLowerCase());
		List<T> list = new ArrayList<>();
		try {
			ModelResource modelResource = (ModelResource) resourceRegistry.getResource(modelName);
			Class<?> clazz = null;
			if (modelResource.getModelClass() != null) {
				clazz = context.getBundle(modelResource.getBundleId()).loadClass(modelResource.getModelClass());
			}
			for (ODocument doc : itr) {
				T model = null;
				if (clazz == null) {
					model = (T) new PersistedModelBase();
				} else {
					model = (T) clazz.newInstance();
				}
				copy(model, doc);
				list.add(model);
			}
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return list;
	}

	@Override
	public <T extends DocumentModel> List<T> find(Class<T> clazz) {
		ORecordIteratorClass<ODocument> itr = docbase
				.browseClass(clazz.getAnnotation(Model.class).name().toLowerCase());
		List<T> list = new ArrayList<>();
		for (ODocument doc : itr) {
			T model = null;
			try {
				model = clazz.newInstance();
				copy(model, doc);
				list.add(model);
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		return list;
	}

	@Override
	public <T> T findById(String modelName, String modelId) {
		List<T> model = findByProperty(modelName, DocumentModel.MODEL_ID, modelId);
		if (model == null || model.size() == 0) {
			return null;
		} else {
			return model.get(0);
		}
	}

	@Override
	public <T> T findById(Class<T> clazz, String modelId) {
		List<T> model = findByProperty(clazz, DocumentModel.MODEL_ID, modelId);
		if (model == null || model.size() == 0) {
			return null;
		} else {
			return model.get(0);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> findByProperty(String modelName, String propertyName, Object value) {
		if (!modelExists(modelName)) {
			throw new RuntimeException("No model with name `" + modelName + "` exists");
		}
		String query = "select from " + modelName.toLowerCase() + " where " + propertyName + " = ?";
		logService.log(LogService.LOG_INFO, "Executing query: " + query);
		List<ODocument> result = docbase.query(new OSQLSynchQuery<>(query), value);

		List<DocumentModel> list = new ArrayList<>();
		try {
			DocumentModel model;
			ModelResource modelResource = (ModelResource) resourceRegistry.getResource(modelName);
			Class<?> clazz = PersistedModelBase.class;
			if (modelResource.getModelClass() != null) {
				clazz = context.getBundle(modelResource.getBundleId()).loadClass(modelResource.getModelClass());
			}
			for (ODocument doc : result) {
				model = (DocumentModel) clazz.newInstance();
				copy(model, doc);
				list.add(model);
			}
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return (List<T>) list;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> findByProperty(Class<T> clazz, String propertyName, Object value) {
		if (!clazz.isAnnotationPresent(Model.class)) {
			throw new RuntimeException("No Model annotation found on class " + clazz.getCanonicalName());
		}
		String modelName = clazz.getAnnotation(Model.class).name();
		if (modelName == null) {
			modelName = clazz.getSimpleName();
		}
		if (!modelExists(modelName)) {
			throw new RuntimeException("No model with name `" + modelName + "` exists");
		}
		String query = "select from " + modelName.toLowerCase() + " where " + propertyName + " = ?";
		logService.log(LogService.LOG_INFO, "Executing query: " + query);
		List<ODocument> result = docbase.query(new OSQLSynchQuery<>(query), value);

		List<DocumentModel> list = new ArrayList<>();
		DocumentModel model;
		for (ODocument doc : result) {
			try {
				model = (DocumentModel) clazz.newInstance();
				copy(model, doc);
				list.add(model);
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		return (List<T>) list;
	}

	@Override
	public <T extends DocumentModel> List<T> findByProperties(String modelName, Map<String, Object> properties) {
		String query = buildQuery(modelName, properties);
		List<T> list = executeQuery(modelName, query, properties);
		return list;
	}

	@Override
	public <T> List<T> findByProperties(Class<T> clazz, Map<String, Object> properties) {
		if (!clazz.isAnnotationPresent(Model.class)) {
			throw new RuntimeException("No Model annotation found on class " + clazz.getCanonicalName());
		}
		String modelName = clazz.getAnnotation(Model.class).name();
		if (modelName == null) {
			modelName = clazz.getSimpleName();
		}
		String query = buildQuery(modelName, properties);
		List<T> list = executeQuery(clazz, query, properties);
		return list;
	}

	@Override
	public <T extends DocumentModel> void delete(String modelName) {
		executeUpdate("delete from " + modelName);
	}

	@Override
	public <T extends DocumentModel> void delete(Class<T> clazz) {
		String modelName = clazz.getAnnotation(Model.class).name();
		delete(modelName);
	}

	@Override
	public <T extends DocumentModel> void delete(String modelName, String modelId) {
		executeUpdate("delete from " + modelName.toLowerCase() + " where " + DocumentModel.MODEL_ID + " = '" + modelId
				+ "'");
	}

	@Override
	public <T extends DocumentModel> void delete(Class<T> clazz, String modelId) {
		String modelName = clazz.getAnnotation(Model.class).name();
		delete(modelName, modelId);
	}

	@Override
	public <T extends DocumentModel> void delete(T documentModel) {
		ORecordId id = new ORecordId(documentModel.getProperty(GREENDB_ID_FIELD).toString());
		docbase.delete(id);
	}

	@Override
	public boolean modelExists(String modelName) {
		return docbase.existsCluster(modelName.toLowerCase());
	}

	@Override
	public void addModel(String modelName) {
		String model = modelName.toLowerCase();
		if (!docbase.existsCluster(model)) {
			executeUpdate("create class " + model);
		}
	}

	@Override
	public CriteriaBuilder getCriteriaBuilder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createQuery(CriteriaUpdate updateQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createQuery(CriteriaDelete deleteQuery) {
		// TODO Auto-generated method stub
		return null;
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

	@Reference(policy = ReferencePolicy.DYNAMIC)
	public void setResourceRegistry(ResourceRegistry resourceRegistry) {
		this.resourceRegistry = resourceRegistry;
	}

	public void unsetResourceRegistry(ResourceRegistry resourceRegistry) {
		this.resourceRegistry = resourceRegistry;
	}

	@Reference(cardinality = ReferenceCardinality.OPTIONAL, policy = ReferencePolicy.DYNAMIC)
	public void setLogService(LogService logService) {
		this.logService = logService;
	}

	public void unsetLogService(LogService logService) {
		this.logService = null;
	}

	private <T extends DocumentModel> T persistNonThreaded(String modelName, T object, boolean create) {
		if (object == null) {
			return null;
		}
		ODocument doc = null;
		if (create) {
			doc = new ODocument(modelName.toLowerCase());
			object.setProperty(DocumentModel.MODEL_ID, generateModelId());
		} else {
			ORecordId id = new ORecordId((String) object.getProperty(GREENDB_ID_FIELD));
			doc = docbase.load(id);
		}
		copy(doc, object.getProperties(), create);
		doc.save(create);
		object.setProperty(GREENDB_ID_FIELD, doc.getIdentity().toString());
		return object;
	}

	private <T extends DocumentModel> T persistNonThreaded(T object, boolean create) {
		if (object == null) {
			return null;
		}
		if (!object.getClass().isAnnotationPresent(Model.class)) {
			throw new RuntimeException("No Model annotation found on class " + object.getClass().getCanonicalName());
		}
		String modelName = object.getClass().getAnnotation(Model.class).name();
		if (modelName == null) {
			modelName = object.getClass().getSimpleName();
		}
		ODocument doc = null;
		if (create) {
			doc = new ODocument(modelName.toLowerCase());
			object.setProperty(DocumentModel.MODEL_ID, generateModelId());
		} else {
			ORecordId id = new ORecordId((String) object.getProperty(GREENDB_ID_FIELD));
			doc = docbase.load(id);
		}
		copy(doc, object.getProperties(), create);
		doc.save(create);
		object.setProperty(GREENDB_ID_FIELD, doc.getIdentity().toString());
		return object;
	}

	private <T extends DocumentModel> void removeNonThreaded(T object) {
		docbase.delete((ORecordId) object.getProperty(GREENDB_ID_FIELD));
	}

	private <T extends DocumentModel> void copy(T model, ODocument doc) throws ClassNotFoundException,
	InstantiationException, IllegalAccessException {
		for (String field : doc.fieldNames()) {
			Object value = doc.field(field);
			if (value instanceof ODocument) {
				ODocument subdoc = (ODocument) value;
				for (Resource resource : resourceRegistry.getResources(ResourceType.Model)) {
					ModelResource modelResource = (ModelResource) resource;
					if (modelResource.getName().toLowerCase().equals(subdoc.getClassName())) {
						Class<?> cls = PersistedModelBase.class;
						if (modelResource.getModelClass() != null) {
							cls = context.getBundle(modelResource.getBundleId()).loadClass(
									modelResource.getModelClass());
						}
						DocumentModel obj = (DocumentModel) cls.newInstance();
						copy(obj, subdoc);
						model.setProperty(field, obj);
						break;
					}
				}
			} else {
				model.setProperty(field, value);
			}
		}
		model.setProperty(GREENDB_ID_FIELD, doc.getIdentity().toString());
	}

	private void copy(ODocument doc, Map<String, Object> properties, boolean create) {
		for (String property : properties.keySet()) {
			Object value = properties.get(property);
			// TODO: what about null values?
			if (value instanceof DocumentModel) {
				DocumentModel model = (DocumentModel) value;
				String modelName = model.getClass().getAnnotation(Model.class).name();
				ODocument subdoc = null;
				if (create) {
					subdoc = new ODocument(modelName.toLowerCase());
				} else {
					subdoc = doc.field(property);
				}
				copy(subdoc, model.getProperties(), create);
				doc.field(property, subdoc);
			} else if (value instanceof Collection<?>) {

			} else {
				doc.field(property, value);
			}
		}

	}

	private String buildQuery(String modelName, Map<String, Object> properties) {
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("select * from ").append(modelName).append(" where ");
		for (String prop : properties.keySet()) {
			Object values = properties.get(prop);
			if (values instanceof Collection) {
				queryBuilder.append(prop).append(" in (:").append(prop).append(")");
			} else {
				queryBuilder.append(prop).append(" = :").append(prop);
			}
			queryBuilder.append(" and ");
		}
		queryBuilder.setLength(queryBuilder.length() - 5);
		return queryBuilder.toString();
	}

	private String generateModelId() {
		return UUID.randomUUID().toString();
	}

}
