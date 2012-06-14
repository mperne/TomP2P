package net.tomp2p.p2p.builder;

import java.util.Set;

import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.EvaluatingSchemeTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;

public class GetTrackerBuilder extends TrackerBuilder<GetTrackerBuilder>
{
	private EvaluatingSchemeTracker evaluatingScheme;
	private Set<Number160> knownPeers;
	private boolean expectAttachement = false;
	private boolean signMessage = false;
	private boolean useSecondaryTrackers = false;
	
	public GetTrackerBuilder(Peer peer, Number160 locationKey)
	{
		super(peer, locationKey);
		self(this);
	}
	
	public EvaluatingSchemeTracker getEvaluatingScheme()
	{
		return evaluatingScheme;
	}

	public GetTrackerBuilder setEvaluatingScheme(EvaluatingSchemeTracker evaluatingScheme)
	{
		this.evaluatingScheme = evaluatingScheme;
		return this;
	}

	public Set<Number160> getKnownPeers()
	{
		return knownPeers;
	}

	public GetTrackerBuilder setKnownPeers(Set<Number160> knownPeers)
	{
		this.knownPeers = knownPeers;
		return this;
	}

	public boolean isExpectAttachement()
	{
		return expectAttachement;
	}
	
	public GetTrackerBuilder setExpectAttachement()
	{
		this.expectAttachement = true;
		return this;
	}
		
	public GetTrackerBuilder setExpectAttachement(boolean expectAttachement)
	{
		this.expectAttachement = expectAttachement;
		return this;
	}

	public boolean isSignMessage()
	{
		return signMessage;
	}
	
	public GetTrackerBuilder setSignMessage()
	{
		this.signMessage = true;
		return this;
	}

	public GetTrackerBuilder setSignMessage(boolean signMessage)
	{
		this.signMessage = signMessage;
		return this;
	}

	public boolean isUseSecondaryTrackers()
	{
		return useSecondaryTrackers;
	}
	
	public GetTrackerBuilder setUseSecondaryTrackers()
	{
		this.useSecondaryTrackers = true;
		return this;
	}

	public GetTrackerBuilder setUseSecondaryTrackers(boolean useSecondaryTrackers)
	{
		this.useSecondaryTrackers = useSecondaryTrackers;
		return this;
	}
	
	public FutureTracker build()
	{
		preBuild("get-tracker-builder");
		
		return peer.getDistributedTracker().getFromTracker(locationKey, domainKey, 
				routingConfiguration, trackerConfiguration, expectAttachement, evaluatingScheme, signMessage, 
				useSecondaryTrackers, knownPeers, futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
	}		
}