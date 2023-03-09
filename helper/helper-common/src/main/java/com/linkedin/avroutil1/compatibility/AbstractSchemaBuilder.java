/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


public abstract class AbstractSchemaBuilder implements SchemaBuilder {

    protected final AvroAdapter _adapter;
    protected Schema.Type _type;
    //used for named types
    protected String _name;
    protected String _namespace;
    protected String _doc;
    protected boolean _isError;
    //used for fixed
    protected int _size;
    //used for enums
    protected List<String> _symbols;
    protected String _defaultSymbol;
    //used for records
    protected List<Schema.Field> _fields = null;
    //used for arrays
    protected Schema _elementSchema;
    //used for maps
    protected Schema _valueSchema;
    //used for unions
    protected List<Schema> _unionBranches;
    //if nested named schema inherit namespace from ancestor
    private boolean _inheritNamespace = false;

    protected AbstractSchemaBuilder(AvroAdapter _adapter, Schema original) {
        this._adapter = _adapter;
        if (original != null) {
            _type = original.getType();

            switch (_type) {
                case FIXED:
                    _name = original.getName();
                    _doc = original.getDoc();
                    _namespace = original.getNamespace();
                    _size = original.getFixedSize();
                    break;
                case ENUM:
                    _name = original.getName();
                    _doc = original.getDoc();
                    _namespace = original.getNamespace();
                    _symbols = original.getEnumSymbols();
                    break;
                case RECORD:
                    _name = original.getName();
                    _doc = original.getDoc();
                    _namespace = original.getNamespace();
                    _isError = original.isError();
                    _fields = new ArrayList<>(original.getFields());
                    break;
                case ARRAY:
                    _elementSchema = original.getElementType();
                    break;
                case MAP:
                    _valueSchema = original.getValueType();
                    break;
                case UNION:
                    _unionBranches = original.getTypes();
                    break;
            }
        }
    }

    @Override
    public SchemaBuilder setType(Schema.Type type) {
        checkSchemaType(type);
        _type = type;
        return this;
    }

    @Override
    public SchemaBuilder setName(String name) {
        _name = name;
        return this;
    }

    @Override
    public SchemaBuilder setNamespace(String namespace) {
        _namespace = namespace;
        return this;
    }

    @Override
    public SchemaBuilder setElementType(Schema elementSchema) {
        _elementSchema = elementSchema;
        return this;
    }

    @Override
    public SchemaBuilder setValueType(Schema valueSchema) {
        _valueSchema = valueSchema;
        return this;
    }

    @Override
    public SchemaBuilder setSize(int size) {
        _size = size;
        return this;
    }

    @Override
    public SchemaBuilder setSymbols(List<String> symbols) {
        _symbols = symbols;
        return this;
    }

    @Override
    public SchemaBuilder setDefaultSymbol(String defaultSymbol) {
        _defaultSymbol = defaultSymbol;
        return this;
    }

    @Override
    public SchemaBuilder addField(Schema.Field field) {
        checkNewField(field);
        if (_fields == null) {
            _fields = new ArrayList<>(1); //assume few fields
        }
        _fields.add(field);
        return this;
    }

    @Override
    public SchemaBuilder addField(int position, Schema.Field field) {
        checkNewField(field);
        if (_fields == null) {
            _fields = new ArrayList<>(1); //assume few fields
        }
        _fields.add(position, field); //will throw IOOB on bad positions
        return this;
    }

    @Override
    public SchemaBuilder removeField(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("argument cannot be null or empty");
        }
        int index = fieldPositionByName(fieldName, false);
        if (index >= 0) {
            _fields.remove(index);
        }
        return this;
    }

    @Override
    public SchemaBuilder removeField(int position) {
        if (_fields ==  null) {
            throw new IndexOutOfBoundsException("current builder has no fields defined. hence position "
                    + position + " is invalid");
        }
        _fields.remove(position); //throws IOOB
        return this;
    }

    @Override
    public SchemaBuilder setUnionBranches(List<Schema> branches) {
        _unionBranches = branches;
        return this;
    }

    @Override
    public SchemaBuilder setInheritNamespace(boolean inheritNamespace) {
        _inheritNamespace = inheritNamespace;
        return this;
    }

    /**
     * {@link Schema.Field} has a position ("pos") property that is set when its added to a schema.
     * this means we need to clone fields to add them to another schema
     * @param originals list of fields to clone
     * @return list of cloned fields
     */
    protected List<Schema.Field> cloneFields(List<Schema.Field> originals) {
        List<Schema.Field> clones = new ArrayList<>(originals.size());
        for (Schema.Field original : originals) {
            FieldBuilder fb = _adapter.newFieldBuilder(original);
            Schema.Field clone = fb.build();
            clones.add(clone);
        }
        return clones;
    }

    protected void checkSchemaType(Schema.Type type) {
        if (type == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
    }

    protected void checkNewField(Schema.Field field) {
        if (field == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        int otherIndex = fieldPositionByName(field.name(), false);
        if (otherIndex >= 0) {
            throw new IllegalArgumentException("schema already contains a field called " + field.name());
        }
    }

    protected int fieldPositionByName(String name, boolean caseSensitive) {
        if (_fields == null) {
            return -1;
        }
        for (int i = 0; i < _fields.size(); i++) {
            Schema.Field candidate = _fields.get(i);
            String cName = candidate.name();
            if (caseSensitive) {
                if (cName.equals(name)) {
                    return i;
                }
            } else if (cName.equalsIgnoreCase(name)){
                return i;
            }
        }
        return -1;
    }

    @Override
    public Schema build() {
        if (_type == null) {
            throw new IllegalArgumentException("type not set");
        }
        Schema result;
        switch (_type) {
            case NULL:
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTES:
            case STRING:
                result = Schema.create(_type);
                break;
            case FIXED:
                validateNames();
                validateSize();
                result = Schema.createFixed(_name, _doc, _namespace, _size);
                break;
            case ENUM:
                validateNames();
                validateSymbols();
                result = buildEnumSchemaInternal();
                break;
            case RECORD:
                validateNames();
                result = Schema.createRecord(_name, _doc, _namespace, _isError);
                if (_fields != null) {
                    List<Schema.Field> fields = _fields.stream()
                        .map(field -> _adapter.newFieldBuilder(field)
                            .setSchema(inheritNamespace(field.schema()))
                            .build())
                        .collect(Collectors.toList());

                    result.setFields(fields);
                }
                break;
            case ARRAY:
                result = Schema.createArray(inheritNamespace(_elementSchema));
                break;
            case MAP:
                result = Schema.createMap(inheritNamespace(_valueSchema));
                break;
            case UNION:
                validateUnionBranches();
                List<Schema> unionBranches = _unionBranches.stream()
                    .map(this::inheritNamespace)
                    .collect(Collectors.toList());
                result = Schema.createUnion(unionBranches);
                break;
            default:
                throw new UnsupportedOperationException("unhandled type " + _type);
        }
        setPropsInternal(result);
        return result;
    }

    private Schema inheritNamespace(Schema schema) {
        if (!_inheritNamespace) {
            return schema;
        }

        SchemaBuilder schemaBuilder = _adapter.newSchemaBuilder(schema).setInheritNamespace(_inheritNamespace);

        //NamedSchema inherits parent's namespace only if it does not have namespace
        if (HelperConsts.NAMED_TYPES.contains(schema.getType())) {
            if (schema.getNamespace() == null) {
                schemaBuilder.setNamespace(_namespace);
            }
        } else {
            schemaBuilder.setNamespace(_namespace);
        }

        return schemaBuilder.build();
    }

    protected Schema buildEnumSchemaInternal() {
        return Schema.createEnum(_name, _doc, _namespace, _symbols);
    }

    protected abstract void setPropsInternal(Schema result);

    protected void validateNames() throws IllegalArgumentException {
        if (_name == null || _name.isEmpty()) {
            throw new IllegalArgumentException("name must not be null or empty");
        }
        validateMultipartName(_name);
        if (_namespace == null || _namespace.isEmpty()) {
            return; //namespace is optional
        }
        boolean fullName = _name.contains(".");
        if (fullName) {
            throw new IllegalArgumentException("namespace " + _namespace + " cannot be used in combination with fullname " + _name);
        }
        validateMultipartName(_namespace);
    }

    protected void  validateMultipartName(String multipart) {
        String[] parts = multipart.split("\\.");
        if (parts.length == 0) {
            throw new IllegalArgumentException("empty name part(s)");
        }
        for (String part : parts) {
            validateNamePart(part);
        }
    }

    protected void validateNamePart(String namePart) throws IllegalArgumentException {
        if (namePart == null || namePart.isEmpty()) {
            throw new IllegalArgumentException("null or empty name part");
        }
        char firstChar = namePart.charAt(0);
        if (!(Character.isLetter(firstChar) || firstChar == '_')) {
            throw new IllegalArgumentException("Illegal initial character '" + firstChar + "' in name part " + namePart);
        }
        for (int i = 1; i < namePart.length(); i++) {
            char c = namePart.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                throw new IllegalArgumentException("Illegal character '" + c + "' in name part " + namePart);
            }
        }
    }

    protected void validateSymbols() throws IllegalArgumentException {
        if (_symbols == null || _symbols.isEmpty()) {
            throw new IllegalArgumentException(">1 unique, non-null symbols required. got " + _symbols);
        }
        HashSet<String> uniqueSymbols = new HashSet<>(_symbols.size());
        for (String symbol : _symbols) {
            if (symbol == null || symbol.isEmpty()) {
                throw new IllegalArgumentException("found null/empty symbol in " + _symbols);
            }
            if (!uniqueSymbols.add(symbol)) {
                throw new IllegalArgumentException("found non-unique symbol " + symbol + " in " + _symbols);
            }
        }
        if (_defaultSymbol != null && !uniqueSymbols.contains(_defaultSymbol)) {
            throw new IllegalArgumentException("default value " + _defaultSymbol + " does not appear in symbols " + _symbols);
        }
    }

    protected void validateUnionBranches() throws IllegalArgumentException {
        if (_unionBranches == null || _unionBranches.isEmpty()) {
            throw new IllegalArgumentException(">1 unique, non-null symbols required. got " + _symbols);
        }
        //from the spec - Unions may not contain more than one schema with the same type, except for the named
        //types record, fixed and enum. For example, unions containing two array types or two map types are not
        //permitted, but two types with different names are permitted.
        Map<String, Schema> uniqueNamedBranches = new HashMap<>(_unionBranches.size());
        HashSet<Schema.Type> uniqueTypes = new HashSet<>(_unionBranches.size());
        for (Schema schema : _unionBranches) {
            if (schema == null) {
                throw new IllegalArgumentException("found null union branch in " + _unionBranches);
            }
            Schema.Type type = schema.getType();
            switch (type) {
                case ENUM:
                case FIXED:
                case RECORD:
                    Schema other = uniqueNamedBranches.put(schema.getFullName(), schema);
                    if (other != null) {
                        throw new IllegalArgumentException("union contains fullname " + schema.getFullName() + " more than once");
                    }
                    break;
                default:
                    if (!uniqueTypes.add(type)) {
                        throw new IllegalArgumentException("union contains type " + type + " more than once");
                    }
                    break;
            }
        }
    }

    protected void validateSize() throws IllegalArgumentException {
        if (_size <= 0) {
            throw new IllegalArgumentException("size value " + _size + " must be >0");
        }
    }
}
