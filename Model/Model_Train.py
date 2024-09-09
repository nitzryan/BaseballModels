import matplotlib.pyplot as plt
import torch
from tqdm import tqdm
from Constants import device

def train(network,  data_generator, loss_function, optimizer, logging = 200, should_output=True):
  network.train() #updates any network layers that behave differently in training and execution
  avg_loss = 0
  num_batches = 0
  for batch, (data, length, target_war, target_level) in enumerate(data_generator):
    data, length = data.to(device), length.to(device)
    target_war, target_level = target_war.to(device), target_level.to(device)
    optimizer.zero_grad()
    output_war, output_level = network(data, length)
    loss_war, loss_level = loss_function(output_war, output_level, target_war, target_level, length)
    loss_war.backward(retain_graph=True)
    loss_level.backward()
    optimizer.step()
    avg_loss += loss_war.item()
    num_batches += 1
    if should_output and ((batch+1)%logging == 0): print('Batch [%d/%d], Train Loss: %.4f' %(batch+1, len(data_generator.dataset)/len(output_war), avg_loss/num_batches))
  return avg_loss/num_batches

def test(network, test_loader, loss_function):
  network.eval() #updates any network layers that behave differently in training and execution
  test_loss = 0
  num_batches = 0
  with torch.no_grad():
    for data, length, target_war, target_level in test_loader:
      data, length = data.to(device), length.to(device)
      target_war, target_level = target_war.to(device), target_level.to(device)
      output_war, output_level = network(data, length)
      loss_war, loss_level = loss_function(output_war, output_level, target_war, target_level, length)
      test_loss += loss_war.item()
      num_batches += 1
  test_loss /= num_batches
  #print('\nTest set: Avg. loss: {:.4f})\n'.format(test_loss))
  return test_loss

def count_parameters(model):
    return sum(p.numel() for p in model.parameters() if p.requires_grad)

def logResults(epoch, num_epochs, train_loss, train_loss_history, test_loss, test_loss_history, epoch_counter, print_interval=1000, should_output=True):
  if should_output and (epoch%print_interval == 0):  
    print('Epoch [%d/%d], Train Loss: %.4f, Test Loss: %.4f' %(epoch+1, num_epochs, train_loss, test_loss))
  train_loss_history.append(train_loss)
  test_loss_history.append(test_loss)
  epoch_counter.append(epoch)

def graphLoss(epoch_counter, train_loss_hist, test_loss_hist, loss_name="Loss", start = 0, graph_y_range=None):
  fig = plt.figure()
  plt.plot(epoch_counter[start:], train_loss_hist[start:], color='blue')
  plt.plot(epoch_counter[start:], test_loss_hist[start:], color='red')
  if graph_y_range is not None:
    plt.ylim(graph_y_range)
  plt.legend(['Train Loss', 'Test Loss'], loc='upper right')
  plt.xlabel('#Epochs')
  plt.ylabel(loss_name)

def trainAndGraph(network, training_generator, testing_generator, loss_function, optimizer, scheduler, num_epochs, logging_interval=1, early_stopping_cutoff=20, should_output=True, graph_y_range=None):
  #Arrays to store training history
  test_loss_history = []
  epoch_counter = []
  train_loss_history = []
  last_loss = 999999
  best_loss = 999999
  best_epoch = 0
  epochsSinceLastImprove = 0
  
  iterable = range(num_epochs)
  if not should_output:
    iterable = tqdm(iterable, leave=False, desc="Training")
  for epoch in iterable:
    avg_loss = train(network, training_generator, loss_function, optimizer, should_output=should_output)
    test_loss = test(network, testing_generator, loss_function)
    scheduler.step(test_loss)
    logResults(epoch, num_epochs, avg_loss, train_loss_history, test_loss, test_loss_history, epoch_counter, logging_interval, should_output)
    # if (test_loss > last_loss):
    #   break
    if (test_loss < best_loss):
      best_loss = test_loss
      best_epoch = epoch
      torch.save(network.state_dict(), 'best_model.pt')
      epochsSinceLastImprove = 0
    else:
      epochsSinceLastImprove += 1
      
    if epochsSinceLastImprove >= early_stopping_cutoff:
      if should_output:
        print("Stopped Training Early")
      break

  if should_output:
    print(f"Best result at epoch={best_epoch} with loss={best_loss}")

    graphLoss(epoch_counter, train_loss_history, test_loss_history, graph_y_range=graph_y_range)
  return best_loss