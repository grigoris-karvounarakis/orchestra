package edu.upenn.cis.orchestra.repository.dao.model.beans;

public class EncapsPeerSchemaIdBean {

	private String _peerId;
	private String _schemaId;
	
	public EncapsPeerSchemaIdBean (String peerId, String schemaId)
	{
		_peerId = peerId;
		_schemaId = schemaId;
	}
	
	public String getPeerId() {
		return _peerId;
	}
	public void setPeerId(String peerId) {
		this._peerId = peerId;
	}
	public String getSchemaId() {
		return _schemaId;
	}
	public void setSchemaId(String schemaId) {
		this._schemaId = schemaId;
	}
	
}
